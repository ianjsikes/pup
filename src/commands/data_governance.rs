use anyhow::Result;
use datadog_api_client::datadogV2::api_sensitive_data_scanner::SensitiveDataScannerAPI;

use crate::client;
use crate::config::Config;
use crate::formatter;

pub async fn scanner_rules_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SensitiveDataScannerAPI::with_client_and_config(dd_cfg, c),
        None => SensitiveDataScannerAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_scanning_groups()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list scanner rules: {e:?}"))?;
    formatter::output(cfg, &resp)
}
