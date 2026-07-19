use crate::model::NeonNewResponse;
use reqwest::{redirect::Policy, Client};
use url::Url;

const NEON_NEW_ENDPOINT: &str = "https://neon.new/api/v1/database";
const HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const USER_AGENT: &str = concat!("neon-localhost/", env!("CARGO_PKG_VERSION"));

pub(crate) fn http_client() -> Result<Client, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    Client::builder()
        .redirect(Policy::none())
        .timeout(HTTP_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()
        .map_err(|error| format!("Could not initialize the network client: {error}"))
}

pub(crate) async fn provision_database(client: &Client) -> Result<NeonNewResponse, String> {
    let response = client
        .post(NEON_NEW_ENDPOINT)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&serde_json::json!({ "ref": "neon-localhost" }))
        .send()
        .await
        .map_err(|error| format!("Could not contact neon.new: {error}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let detail = concise_http_error(&response.text().await.unwrap_or_default());
        return Err(if detail.is_empty() {
            format!("neon.new returned {status}")
        } else {
            format!("neon.new returned {status}: {detail}")
        });
    }

    response
        .json()
        .await
        .map_err(|error| format!("Could not read the neon.new response: {error}"))
}

pub(crate) async fn resolve_claim_url(client: &Client, claim_url: &str) -> Result<String, String> {
    let claim_url = validated_claim_url(claim_url, "neon.new", "/claim/")?;
    let response = client
        .get(claim_url.clone())
        .send()
        .await
        .map_err(|error| format!("Could not start the Neon claim flow: {error}"))?;

    if !response.status().is_redirection() {
        return Err(format!(
            "Neon could not start the claim flow (HTTP {}). The database is still available locally.",
            response.status().as_u16()
        ));
    }

    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "Neon did not provide a claim destination".to_string())?;
    let destination = claim_url
        .join(location)
        .map_err(|_| "Neon returned an invalid claim destination".to_string())?;
    validated_claim_url(destination.as_str(), "console.neon.tech", "/app/claim")
        .map(|url| url.to_string())
}

fn validated_claim_url(
    value: &str,
    expected_host: &str,
    expected_path: &str,
) -> Result<Url, String> {
    let url = Url::parse(value).map_err(|_| "Neon returned an invalid claim URL".to_string())?;
    let path_matches = if expected_path.ends_with('/') {
        url.path().starts_with(expected_path)
    } else {
        url.path() == expected_path
    };
    if url.scheme() != "https" || url.host_str() != Some(expected_host) || !path_matches {
        return Err("Neon returned an unexpected claim destination".to_string());
    }
    Ok(url)
}

fn concise_http_error(detail: &str) -> String {
    const MAX_CHARS: usize = 300;
    let normalized = detail.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MAX_CHARS {
        normalized
    } else {
        format!(
            "{}…",
            normalized.chars().take(MAX_CHARS).collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_initializes_with_the_selected_crypto_provider() {
        http_client().expect("HTTP client");
    }

    #[test]
    fn claim_urls_are_limited_to_exact_neon_claim_pages() {
        assert!(validated_claim_url(
            "https://neon.new/claim/019f778c-cbe0-74bc-8ee6-84bff839d74c",
            "neon.new",
            "/claim/"
        )
        .is_ok());
        assert!(validated_claim_url(
            "https://console.neon.tech/app/claim?p=project&tr=transfer",
            "console.neon.tech",
            "/app/claim"
        )
        .is_ok());
        for url in [
            "https://example.com/app/claim?p=project",
            "http://console.neon.tech/app/claim?p=project",
            "https://console.neon.tech/app/claim-elsewhere?p=project",
        ] {
            assert!(validated_claim_url(url, "console.neon.tech", "/app/claim").is_err());
        }
    }

    #[test]
    fn http_error_details_are_bounded_and_single_line() {
        let detail = format!("first\nsecond {}", "x".repeat(400));
        let concise = concise_http_error(&detail);
        assert!(!concise.contains('\n'));
        assert!(concise.ends_with('…'));
        assert_eq!(concise.chars().count(), 301);
    }
}
