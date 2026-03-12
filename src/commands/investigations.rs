use anyhow::Result;

use crate::client;
use crate::config::Config;
use crate::formatter;

pub async fn list(cfg: &Config, page_limit: i64, page_offset: i64) -> Result<()> {
    let path = format!(
        "/api/v2/bits-ai/investigations?page[limit]={page_limit}&page[offset]={page_offset}"
    );
    let data = client::raw_get(cfg, &path).await?;
    formatter::output(cfg, &data)
}

pub async fn get(cfg: &Config, investigation_id: &str) -> Result<()> {
    let path = format!("/api/unstable/bits-ai/investigation/{investigation_id}");
    let data = client::raw_get(cfg, &path).await?;
    formatter::output(cfg, &data)
}

pub async fn trigger(cfg: &Config, file: &str) -> Result<()> {
    let body: serde_json::Value = crate::util::read_json_file(file)?;
    let data = client::raw_post(cfg, "/api/v2/bits-ai/investigations", body).await?;
    formatter::output(cfg, &data)
}
