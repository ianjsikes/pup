use anyhow::Result;
use datadog_api_client::datadogV2::api_microsoft_teams_integration::{
    ListTenantBasedHandlesOptionalParams, ListWorkflowsWebhookHandlesOptionalParams,
    MicrosoftTeamsIntegrationAPI,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

// ---------------------------------------------------------------------------
// Helper: build a MicrosoftTeamsIntegrationAPI with bearer-token support
// ---------------------------------------------------------------------------

fn make_api(cfg: &Config) -> MicrosoftTeamsIntegrationAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => MicrosoftTeamsIntegrationAPI::with_client_and_config(dd_cfg, c),
        None => MicrosoftTeamsIntegrationAPI::with_config(dd_cfg),
    }
}

// ---------------------------------------------------------------------------
// Tenant-based handles (channels)
// ---------------------------------------------------------------------------

pub async fn handles_list(cfg: &Config) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .list_tenant_based_handles(ListTenantBasedHandlesOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list MS Teams handles: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_get(cfg: &Config, handle_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_tenant_based_handle(handle_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get MS Teams handle: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_create(cfg: &Config, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_tenant_based_handle(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create MS Teams handle: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_update(cfg: &Config, handle_id: &str, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_tenant_based_handle(handle_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update MS Teams handle: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_delete(cfg: &Config, handle_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_tenant_based_handle(handle_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete MS Teams handle: {:?}", e))?;
    println!("MS Teams handle {handle_id} deleted.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Channel lookup by name
// ---------------------------------------------------------------------------

pub async fn channel_get_by_name(
    cfg: &Config,
    tenant_name: &str,
    team_name: &str,
    channel_name: &str,
) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_channel_by_name(
            tenant_name.to_string(),
            team_name.to_string(),
            channel_name.to_string(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to get MS Teams channel by name: {:?}", e))?;
    formatter::output(cfg, &resp)
}

// ---------------------------------------------------------------------------
// Workflows webhook handles
// ---------------------------------------------------------------------------

pub async fn workflows_list(cfg: &Config) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .list_workflows_webhook_handles(ListWorkflowsWebhookHandlesOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list MS Teams workflow handles: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn workflows_get(cfg: &Config, handle_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_workflows_webhook_handle(handle_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get MS Teams workflow handle: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn workflows_create(cfg: &Config, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_workflows_webhook_handle(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create MS Teams workflow handle: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn workflows_update(cfg: &Config, handle_id: &str, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_workflows_webhook_handle(handle_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update MS Teams workflow handle: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn workflows_delete(cfg: &Config, handle_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_workflows_webhook_handle(handle_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete MS Teams workflow handle: {:?}", e))?;
    println!("MS Teams workflow handle {handle_id} deleted.");
    Ok(())
}
