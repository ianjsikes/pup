use anyhow::Result;
use datadog_api_client::datadogV2::api_authn_mappings::{
    AuthNMappingsAPI, ListAuthNMappingsOptionalParams,
};
use datadog_api_client::datadogV2::model::{AuthNMappingCreateRequest, AuthNMappingUpdateRequest};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AuthNMappingsAPI::with_client_and_config(dd_cfg, c),
        None => AuthNMappingsAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_authn_mappings(ListAuthNMappingsOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list AuthN mappings: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn get(cfg: &Config, mapping_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AuthNMappingsAPI::with_client_and_config(dd_cfg, c),
        None => AuthNMappingsAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_authn_mapping(mapping_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get AuthN mapping: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn create(cfg: &Config, file: &str) -> Result<()> {
    let body: AuthNMappingCreateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AuthNMappingsAPI::with_client_and_config(dd_cfg, c),
        None => AuthNMappingsAPI::with_config(dd_cfg),
    };
    let resp = api
        .create_authn_mapping(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create AuthN mapping: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn update(cfg: &Config, mapping_id: &str, file: &str) -> Result<()> {
    let body: AuthNMappingUpdateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AuthNMappingsAPI::with_client_and_config(dd_cfg, c),
        None => AuthNMappingsAPI::with_config(dd_cfg),
    };
    let resp = api
        .update_authn_mapping(mapping_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update AuthN mapping: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn delete(cfg: &Config, mapping_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AuthNMappingsAPI::with_client_and_config(dd_cfg, c),
        None => AuthNMappingsAPI::with_config(dd_cfg),
    };
    api.delete_authn_mapping(mapping_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete AuthN mapping: {e:?}"))?;
    println!("AuthN mapping '{mapping_id}' deleted.");
    Ok(())
}
