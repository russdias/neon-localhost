use crate::model::{DatabaseStorage, Upstream};
use percent_encoding::percent_decode_str;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256, SCRAM_SHA_256};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::{timeout, Duration},
};
use tokio_native_tls::{TlsConnector, TlsStream};
use url::Url;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);
const QUERY_TIMEOUT: Duration = Duration::from_secs(20);
const LOCAL_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_STARTUP_PACKET_SIZE: usize = 64 * 1024;
const POSTGRES_PROTOCOL_V3: u32 = 196_608;
const SSL_REQUEST_CODE: u32 = 80_877_103;
const GSSENC_REQUEST_CODE: u32 = 80_877_104;
const CANCEL_REQUEST_CODE: u32 = 80_877_102;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct CancelKey {
    pub(crate) process_id: u32,
    pub(crate) secret_key: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CancelTarget {
    pub(crate) host: String,
    pub(crate) port: u16,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LocalStartup {
    Session,
    Cancel(CancelKey),
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}

pub(crate) fn parse_upstream_for_port(
    connection_string: &str,
    local_port: u16,
) -> Result<(Upstream, String), String> {
    let mut url = Url::parse(connection_string)
        .map_err(|error| format!("Neon returned an invalid connection URL: {error}"))?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return Err("Neon returned an unsupported connection URL".to_string());
    }

    let host = url
        .host_str()
        .ok_or_else(|| "The Neon connection URL does not contain a hostname".to_string())?
        .to_ascii_lowercase();
    if host != "neon.tech" && !host.ends_with(".neon.tech") {
        return Err("Neon returned a connection URL outside neon.tech".to_string());
    }
    let decode = |value: &str| percent_decode_str(value).decode_utf8_lossy().into_owned();
    let user = decode(url.username());
    let password = decode(
        url.password()
            .ok_or_else(|| "The Neon connection URL does not contain a password".to_string())?,
    );
    let database = decode(url.path().trim_start_matches('/'));
    if user.is_empty() || database.is_empty() {
        return Err("The Neon connection URL is missing a user or database".to_string());
    }
    if user.contains('\0') || database.contains('\0') {
        return Err("The Neon connection URL contains invalid credentials".to_string());
    }
    if url.fragment().is_some() {
        return Err("The Neon connection URL contains an unexpected fragment".to_string());
    }

    let upstream = Upstream {
        host,
        port: url.port().unwrap_or(5432),
        user,
        password,
        database,
    };
    url.set_host(Some("localhost"))
        .map_err(|_| "Could not create the local connection URL".to_string())?;
    url.set_port(Some(local_port))
        .map_err(|_| "Could not set the local proxy port".to_string())?;
    url.set_username("")
        .map_err(|_| "Could not create the passwordless local URL".to_string())?;
    url.set_password(None)
        .map_err(|_| "Could not create the passwordless local URL".to_string())?;
    url.set_query(None);
    url.query_pairs_mut().append_pair("sslmode", "disable");
    Ok((upstream, url.to_string()))
}

async fn connect_endpoint(host: &str, port: u16) -> Result<TcpStream, String> {
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| "Timed out while connecting to Neon".to_string())?
        .map_err(|error| format!("Could not reach Neon: {error}"))?;
    stream
        .set_nodelay(true)
        .map_err(|error| format!("Could not configure the Neon connection: {error}"))?;
    Ok(stream)
}

async fn connect_tls_endpoint(host: &str, port: u16) -> Result<TlsStream<TcpStream>, String> {
    let mut stream = connect_endpoint(host, port).await?;
    stream
        .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
        .await
        .map_err(|error| format!("Could not request Neon TLS: {error}"))?;
    let mut response = [0_u8; 1];
    stream
        .read_exact(&mut response)
        .await
        .map_err(|error| format!("Neon did not answer the TLS request: {error}"))?;
    if response[0] != b'S' {
        return Err("Neon refused the secure database connection".to_string());
    }
    let connector = native_tls::TlsConnector::builder()
        .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
        .build()
        .map_err(|error| format!("Could not initialize TLS: {error}"))?;
    TlsConnector::from(connector)
        .connect(host, stream)
        .await
        .map_err(|error| format!("Could not establish Neon TLS: {error}"))
}

async fn read_backend_message<R: AsyncRead + Unpin>(stream: &mut R) -> Result<Vec<u8>, String> {
    let mut header = [0_u8; 5];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| format!("Could not read Neon response: {error}"))?;
    let length = read_u32(&header, 1)
        .ok_or_else(|| "Neon returned an invalid Postgres message".to_string())?
        as usize;
    if !(4..=16 * 1024 * 1024).contains(&length) {
        return Err("Neon returned an invalid Postgres message".to_string());
    }
    let mut message = Vec::with_capacity(length + 1);
    message.extend_from_slice(&header);
    message.resize(length + 1, 0);
    stream
        .read_exact(&mut message[5..])
        .await
        .map_err(|error| format!("Could not read Neon response: {error}"))?;
    Ok(message)
}

fn parse_storage_row(message: &[u8]) -> Result<DatabaseStorage, String> {
    if message.len() < 7 || message[0] != b'D' || message[5..7] != 2_u16.to_be_bytes() {
        return Err("Postgres returned an invalid storage result".to_string());
    }
    let mut cursor = 7;
    let mut values = [0_u64; 2];
    for value in &mut values {
        let length = read_u32(message, cursor)
            .ok_or_else(|| "Postgres returned an incomplete storage result".to_string())?
            as i32;
        cursor += 4;
        let end = (length >= 0)
            .then(|| cursor.checked_add(length as usize))
            .flatten()
            .filter(|end| *end <= message.len())
            .ok_or_else(|| "Postgres returned an invalid storage value".to_string())?;
        let text = std::str::from_utf8(&message[cursor..end])
            .map_err(|_| "Postgres returned a non-text storage value".to_string())?;
        *value = text
            .parse()
            .map_err(|_| "Postgres returned a non-numeric storage value".to_string())?;
        cursor = end;
    }
    if cursor != message.len() {
        return Err("Postgres returned trailing storage data".to_string());
    }
    Ok(DatabaseStorage {
        used_bytes: values[0],
        limit_bytes: values[1],
    })
}

pub(crate) async fn read_database_storage(upstream: &Upstream) -> Result<DatabaseStorage, String> {
    timeout(QUERY_TIMEOUT, async {
        let (mut stream, _) = authenticate_upstream(upstream).await?;
        let query = "select pg_database_size(current_database()), pg_size_bytes(current_setting('neon.max_cluster_size'))";
        let mut message = Vec::with_capacity(query.len() + 6);
        message.push(b'Q');
        message.extend_from_slice(&((query.len() + 5) as u32).to_be_bytes());
        message.extend_from_slice(query.as_bytes());
        message.push(0);
        stream
            .write_all(&message)
            .await
            .map_err(|error| format!("Could not request database storage: {error}"))?;
        let mut storage = None;
        loop {
            let response = read_backend_message(&mut stream).await?;
            match response[0] {
                b'D' => storage = Some(parse_storage_row(&response)?),
                b'E' => return Err("Neon could not report database storage".to_string()),
                b'Z' => return storage.ok_or_else(|| "Neon did not return database storage".to_string()),
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| "Timed out while reading database storage".to_string())?
}

fn password_message(payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(payload.len() + 5);
    message.push(b'p');
    message.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    message.extend_from_slice(payload);
    message
}

pub(crate) async fn authenticate_upstream(
    upstream: &Upstream,
) -> Result<(TlsStream<TcpStream>, Vec<Vec<u8>>), String> {
    timeout(HANDSHAKE_TIMEOUT, async {
        let mut stream = connect_tls_endpoint(&upstream.host, upstream.port).await?;
        let mut payload = Vec::new();
        payload.extend_from_slice(&POSTGRES_PROTOCOL_V3.to_be_bytes());
        for (key, value) in [
            ("user", upstream.user.as_str()),
            ("database", upstream.database.as_str()),
            ("application_name", "neon-localhost"),
        ] {
            payload.extend_from_slice(key.as_bytes());
            payload.push(0);
            payload.extend_from_slice(value.as_bytes());
            payload.push(0);
        }
        payload.push(0);
        let mut startup = Vec::with_capacity(payload.len() + 4);
        startup.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        startup.extend_from_slice(&payload);
        stream
            .write_all(&startup)
            .await
            .map_err(|error| format!("Could not start the Neon session: {error}"))?;

        let mut scram: Option<ScramSha256> = None;
        let mut messages = Vec::new();
        loop {
            let message = read_backend_message(&mut stream).await?;
            match message[0] {
                b'R' => {
                    let auth_type = read_u32(&message, 5).ok_or_else(|| {
                        "Neon returned an invalid authentication message".to_string()
                    })?;
                    match auth_type {
                        0 => messages.push(message),
                        10 => {
                            if !message[9..]
                                .split(|byte| *byte == 0)
                                .any(|item| item == SCRAM_SHA_256.as_bytes())
                            {
                                return Err("Neon did not offer supported password authentication"
                                    .to_string());
                            }
                            let client = ScramSha256::new(
                                upstream.password.as_bytes(),
                                ChannelBinding::unsupported(),
                            );
                            let mut response = Vec::new();
                            response.extend_from_slice(SCRAM_SHA_256.as_bytes());
                            response.push(0);
                            response
                                .extend_from_slice(&(client.message().len() as u32).to_be_bytes());
                            response.extend_from_slice(client.message());
                            stream
                                .write_all(&password_message(&response))
                                .await
                                .map_err(|error| {
                                    format!("Could not authenticate with Neon: {error}")
                                })?;
                            scram = Some(client);
                        }
                        11 => {
                            let client = scram.as_mut().ok_or_else(|| {
                                "Unexpected Neon authentication response".to_string()
                            })?;
                            client.update(&message[9..]).map_err(|error| {
                                format!("Invalid Neon authentication response: {error}")
                            })?;
                            stream
                                .write_all(&password_message(client.message()))
                                .await
                                .map_err(|error| {
                                    format!("Could not authenticate with Neon: {error}")
                                })?;
                        }
                        12 => {
                            scram
                                .as_mut()
                                .ok_or_else(|| {
                                    "Unexpected Neon authentication response".to_string()
                                })?
                                .finish(&message[9..])
                                .map_err(|error| {
                                    format!("Neon password verification failed: {error}")
                                })?;
                        }
                        _ => {
                            return Err(format!(
                                "Neon requested unsupported authentication type {auth_type}"
                            ))
                        }
                    }
                }
                b'E' => return Err("Neon rejected the database credentials".to_string()),
                b'Z' => {
                    messages.push(message);
                    return Ok((stream, messages));
                }
                _ => messages.push(message),
            }
        }
    })
    .await
    .map_err(|_| "Timed out while establishing the Neon database session".to_string())?
}

pub(crate) fn backend_cancel_key(message: &[u8]) -> Option<CancelKey> {
    if message.len() != 13 || message[0] != b'K' || read_u32(message, 1) != Some(12) {
        return None;
    }
    Some(CancelKey {
        process_id: read_u32(message, 5)?,
        secret_key: read_u32(message, 9)?,
    })
}

pub(crate) async fn forward_cancel(target: &CancelTarget, key: CancelKey) -> Result<(), String> {
    timeout(HANDSHAKE_TIMEOUT, async {
        let mut stream = connect_tls_endpoint(&target.host, target.port).await?;
        let mut packet = [0_u8; 16];
        packet[..4].copy_from_slice(&16_u32.to_be_bytes());
        packet[4..8].copy_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        packet[8..12].copy_from_slice(&key.process_id.to_be_bytes());
        packet[12..].copy_from_slice(&key.secret_key.to_be_bytes());
        stream.write_all(&packet).await.map_err(|error| {
            format!("Could not forward the query cancellation to Neon: {error}")
        })?;
        stream
            .shutdown()
            .await
            .map_err(|error| format!("Could not finish the query cancellation: {error}"))
    })
    .await
    .map_err(|_| "Timed out while forwarding the query cancellation".to_string())?
}

pub(crate) async fn read_local_startup<S>(local: &mut S) -> Result<LocalStartup, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    timeout(LOCAL_STARTUP_TIMEOUT, async {
        let mut first_packet = [0_u8; 8];
        local
            .read_exact(&mut first_packet)
            .await
            .map_err(|error| format!("Could not read local Postgres startup: {error}"))?;
        while read_u32(&first_packet, 0) == Some(8)
            && matches!(
                read_u32(&first_packet, 4),
                Some(SSL_REQUEST_CODE) | Some(GSSENC_REQUEST_CODE)
            )
        {
            local
                .write_all(b"N")
                .await
                .map_err(|error| format!("Could not answer local encryption request: {error}"))?;
            local
                .read_exact(&mut first_packet)
                .await
                .map_err(|error| format!("Could not read local Postgres startup: {error}"))?;
        }
        let packet_length = read_u32(&first_packet, 0)
            .ok_or_else(|| "Invalid local Postgres startup packet".to_string())?
            as usize;
        let request_code = read_u32(&first_packet, 4)
            .ok_or_else(|| "Invalid local Postgres startup packet".to_string())?;
        if packet_length == 16 && request_code == CANCEL_REQUEST_CODE {
            let mut payload = [0_u8; 8];
            local
                .read_exact(&mut payload)
                .await
                .map_err(|error| format!("Could not read local cancellation request: {error}"))?;
            return Ok(LocalStartup::Cancel(CancelKey {
                process_id: read_u32(&payload, 0)
                    .ok_or_else(|| "Invalid local cancellation request".to_string())?,
                secret_key: read_u32(&payload, 4)
                    .ok_or_else(|| "Invalid local cancellation request".to_string())?,
            }));
        }
        if request_code != POSTGRES_PROTOCOL_V3
            || !(9..=MAX_STARTUP_PACKET_SIZE).contains(&packet_length)
        {
            return Err("Invalid local Postgres startup packet".to_string());
        }
        let mut ignored = vec![0_u8; packet_length - 8];
        local
            .read_exact(&mut ignored)
            .await
            .map_err(|error| format!("Could not read local Postgres startup: {error}"))?;
        Ok(LocalStartup::Session)
    })
    .await
    .map_err(|_| "Timed out waiting for the local Postgres startup packet".to_string())?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_urls_hide_credentials_and_restrict_destinations() {
        let remote =
            "postgresql://neon%20user:p%40ssword@ep-example.neon.tech:5432/my%2Ddb?sslmode=require";
        let (upstream, local) = parse_upstream_for_port(remote, 15_432).expect("valid URL");
        assert_eq!(upstream.user, "neon user");
        assert_eq!(upstream.password, "p@ssword");
        assert_eq!(
            local,
            "postgresql://localhost:15432/my%2Ddb?sslmode=disable"
        );
        assert!(!local.contains("p%40ssword"));

        for unsafe_url in [
            "https://user:password@ep-example.neon.tech/neondb",
            "postgresql://user:password@example.com/neondb",
            "postgresql://user:password@neon.tech.evil.example/neondb",
            "postgresql://:password@ep-example.neon.tech/neondb",
            "postgresql://user:password@ep-example.neon.tech/",
            "postgresql://us%00er:password@ep-example.neon.tech/neondb",
        ] {
            assert!(parse_upstream_for_port(unsafe_url, 5432).is_err());
        }
    }

    #[test]
    fn storage_rows_and_backend_keys_are_parsed_strictly() {
        let mut row = vec![b'D', 0, 0, 0, 0, 0, 2];
        for value in [b"82386944".as_slice(), b"104857600".as_slice()] {
            row.extend_from_slice(&(value.len() as i32).to_be_bytes());
            row.extend_from_slice(value);
        }
        let row_length = (row.len() - 1) as u32;
        row[1..5].copy_from_slice(&row_length.to_be_bytes());
        assert_eq!(parse_storage_row(&row).expect("row").used_bytes, 82_386_944);
        row.push(0);
        assert!(parse_storage_row(&row).is_err());

        let mut key = vec![b'K'];
        key.extend_from_slice(&12_u32.to_be_bytes());
        key.extend_from_slice(&1234_u32.to_be_bytes());
        key.extend_from_slice(&5678_u32.to_be_bytes());
        assert_eq!(
            backend_cancel_key(&key),
            Some(CancelKey {
                process_id: 1234,
                secret_key: 5678
            })
        );
    }

    #[tokio::test]
    async fn startup_negotiates_local_ssl_and_parses_cancellation() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let task = tokio::spawn(async move {
            client
                .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
                .await
                .unwrap();
            let mut response = [0];
            client.read_exact(&mut response).await.unwrap();
            assert_eq!(response, [b'N']);
            client
                .write_all(&[0, 0, 0, 9, 0, 3, 0, 0, 0])
                .await
                .unwrap();
        });
        assert_eq!(
            read_local_startup(&mut server).await.unwrap(),
            LocalStartup::Session
        );
        task.await.unwrap();

        let (mut client, mut server) = tokio::io::duplex(32);
        let task = tokio::spawn(async move {
            let mut packet = [0_u8; 16];
            packet[..4].copy_from_slice(&16_u32.to_be_bytes());
            packet[4..8].copy_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
            packet[8..12].copy_from_slice(&1234_u32.to_be_bytes());
            packet[12..].copy_from_slice(&5678_u32.to_be_bytes());
            client.write_all(&packet).await.unwrap();
        });
        assert_eq!(
            read_local_startup(&mut server).await.unwrap(),
            LocalStartup::Cancel(CancelKey {
                process_id: 1234,
                secret_key: 5678
            })
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn startup_rejects_unknown_protocols_and_oversized_packets() {
        for header in [[0, 0, 0, 9, 0, 4, 0, 0], [0, 1, 0, 1, 0, 3, 0, 0]] {
            let (mut client, mut server) = tokio::io::duplex(16);
            client.write_all(&header).await.unwrap();
            assert!(read_local_startup(&mut server).await.is_err());
        }
    }
}
