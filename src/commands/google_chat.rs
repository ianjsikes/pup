use anyhow::Result;
use datadog_api_client::datadogV2::api_google_chat_integration::GoogleChatIntegrationAPI;
use datadog_api_client::datadogV2::model::{
    GoogleChatCreateOrganizationHandleRequest, GoogleChatUpdateOrganizationHandleRequest,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn make_api(cfg: &Config) -> GoogleChatIntegrationAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => GoogleChatIntegrationAPI::with_client_and_config(dd_cfg, c),
        None => GoogleChatIntegrationAPI::with_config(dd_cfg),
    }
}

pub async fn handles_list(cfg: &Config, org_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .list_organization_handles(org_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list organization handles: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_get(cfg: &Config, org_id: &str, handle_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_organization_handle(org_id.to_string(), handle_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get organization handle: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_create(cfg: &Config, org_id: &str, file: &str) -> Result<()> {
    let body: GoogleChatCreateOrganizationHandleRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_organization_handle(org_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create organization handle: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_update(cfg: &Config, org_id: &str, handle_id: &str, file: &str) -> Result<()> {
    let body: GoogleChatUpdateOrganizationHandleRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_organization_handle(org_id.to_string(), handle_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update organization handle: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_delete(cfg: &Config, org_id: &str, handle_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_organization_handle(org_id.to_string(), handle_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete organization handle: {e:?}"))?;
    println!("Handle '{handle_id}' deleted.");
    Ok(())
}

pub async fn space_get(cfg: &Config, domain_name: &str, space_display_name: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_space_by_display_name(domain_name.to_string(), space_display_name.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get space: {e:?}"))?;
    formatter::output(cfg, &resp)
}
