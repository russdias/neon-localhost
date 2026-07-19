use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub(crate) struct NeonNewResponse {
    pub(crate) status: String,
    pub(crate) neon_project_id: String,
    pub(crate) connection_string: String,
    pub(crate) claim_url: String,
    pub(crate) expires_at: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LocalDatabase {
    pub(crate) status: String,
    pub(crate) project_id: String,
    pub(crate) local_url: String,
    pub(crate) remote_url: String,
    pub(crate) claim_url: String,
    pub(crate) expires_at: String,
    pub(crate) port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Upstream {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatabaseStorage {
    pub(crate) used_bytes: u64,
    pub(crate) limit_bytes: u64,
}
