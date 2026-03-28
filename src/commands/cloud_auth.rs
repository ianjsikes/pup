use anyhow::Result;
use datadog_api_client::datadogV2::api_cloud_authentication::CloudAuthenticationAPI;
use datadog_api_client::datadogV2::model::AWSCloudAuthPersonaMappingCreateRequest;

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn make_api(cfg: &Config) -> CloudAuthenticationAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => CloudAuthenticationAPI::with_client_and_config(dd_cfg, c),
        None => CloudAuthenticationAPI::with_config(dd_cfg),
    }
}

pub async fn persona_mappings_list(cfg: &Config) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .list_aws_cloud_auth_persona_mappings()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list persona mappings: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn persona_mappings_get(cfg: &Config, mapping_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_aws_cloud_auth_persona_mapping(mapping_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get persona mapping: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn persona_mappings_create(cfg: &Config, file: &str) -> Result<()> {
    let body: AWSCloudAuthPersonaMappingCreateRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_aws_cloud_auth_persona_mapping(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create persona mapping: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn persona_mappings_delete(cfg: &Config, mapping_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_aws_cloud_auth_persona_mapping(mapping_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete persona mapping: {e:?}"))?;
    println!("Persona mapping '{mapping_id}' deleted.");
    Ok(())
}
