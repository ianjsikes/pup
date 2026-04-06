use anyhow::Result;
use datadog_api_client::datadogV2::api_processes::{ListProcessesOptionalParams, ProcessesAPI};

use crate::client;
use crate::config::Config;
use crate::formatter;

pub async fn list(
    cfg: &Config,
    search: Option<String>,
    tags: Option<String>,
    page_limit: Option<i32>,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ProcessesAPI::with_client_and_config(dd_cfg, c),
        None => ProcessesAPI::with_config(dd_cfg),
    };
    let mut params = ListProcessesOptionalParams::default();
    if let Some(s) = search {
        params = params.search(s);
    }
    if let Some(t) = tags {
        params = params.tags(t);
    }
    if let Some(pl) = page_limit {
        params = params.page_limit(pl);
    }
    let resp = api
        .list_processes(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list processes: {e:?}"))?;
    formatter::output(cfg, &resp)
}
