use anyhow::Result;
use datadog_api_client::datadogV1::api_dashboards::{DashboardsAPI, ListDashboardsOptionalParams};
use datadog_api_client::datadogV1::model::Dashboard;

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => DashboardsAPI::with_client_and_config(dd_cfg, c),
        None => DashboardsAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_dashboards(ListDashboardsOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list dashboards: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn get(cfg: &Config, id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => DashboardsAPI::with_client_and_config(dd_cfg, c),
        None => DashboardsAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_dashboard(id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get dashboard: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn create(cfg: &Config, file: &str) -> Result<()> {
    let body: Dashboard = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => DashboardsAPI::with_client_and_config(dd_cfg, c),
        None => DashboardsAPI::with_config(dd_cfg),
    };
    let resp = api
        .create_dashboard(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create dashboard: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn update(cfg: &Config, id: &str, file: &str) -> Result<()> {
    let body: Dashboard = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => DashboardsAPI::with_client_and_config(dd_cfg, c),
        None => DashboardsAPI::with_config(dd_cfg),
    };
    let resp = api
        .update_dashboard(id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update dashboard: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn delete(cfg: &Config, id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => DashboardsAPI::with_client_and_config(dd_cfg, c),
        None => DashboardsAPI::with_config(dd_cfg),
    };
    let resp = api
        .delete_dashboard(id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete dashboard: {e:?}"))?;
    formatter::output(cfg, &resp)
}
