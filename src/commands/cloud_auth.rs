use anyhow::Result;
#[cfg(not(target_arch = "wasm32"))]
use datadog_api_client::datadogV2::api_cloud_authentication::CloudAuthenticationAPI;
#[cfg(not(target_arch = "wasm32"))]
use datadog_api_client::datadogV2::model::AWSCloudAuthPersonaMappingCreateRequest;

#[cfg(not(target_arch = "wasm32"))]
use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

#[cfg(not(target_arch = "wasm32"))]
fn make_api(cfg: &Config) -> CloudAuthenticationAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => CloudAuthenticationAPI::with_client_and_config(dd_cfg, c),
        None => CloudAuthenticationAPI::with_config(dd_cfg),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn persona_mappings_list(cfg: &Config) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .list_aws_cloud_auth_persona_mappings()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list persona mappings: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn persona_mappings_list(cfg: &Config) -> Result<()> {
    let data = crate::api::get(cfg, "/api/v2/cloud_auth/aws/persona_mapping", &[]).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn persona_mappings_get(cfg: &Config, mapping_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_aws_cloud_auth_persona_mapping(mapping_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get persona mapping: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn persona_mappings_get(cfg: &Config, mapping_id: &str) -> Result<()> {
    let data = crate::api::get(
        cfg,
        &format!("/api/v2/cloud_auth/aws/persona_mapping/{mapping_id}"),
        &[],
    )
    .await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn persona_mappings_create(cfg: &Config, file: &str) -> Result<()> {
    let body: AWSCloudAuthPersonaMappingCreateRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_aws_cloud_auth_persona_mapping(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create persona mapping: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn persona_mappings_create(cfg: &Config, file: &str) -> Result<()> {
    let body: serde_json::Value = util::read_json_file(file)?;
    let data = crate::api::post(cfg, "/api/v2/cloud_auth/aws/persona_mapping", &body).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn persona_mappings_delete(cfg: &Config, mapping_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_aws_cloud_auth_persona_mapping(mapping_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete persona mapping: {e:?}"))?;
    println!("Persona mapping '{mapping_id}' deleted.");
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub async fn persona_mappings_delete(cfg: &Config, mapping_id: &str) -> Result<()> {
    crate::api::delete(
        cfg,
        &format!("/api/v2/cloud_auth/aws/persona_mapping/{mapping_id}"),
    )
    .await?;
    println!("Persona mapping '{mapping_id}' deleted.");
    Ok(())
}
