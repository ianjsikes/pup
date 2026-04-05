use anyhow::Result;
use datadog_api_client::datadogV2::api_software_catalog::{
    ListCatalogEntityOptionalParams, ListCatalogKindOptionalParams,
    ListCatalogRelationOptionalParams, SoftwareCatalogAPI,
};
use datadog_api_client::datadogV2::model::{UpsertCatalogEntityRequest, UpsertCatalogKindRequest};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn entities_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SoftwareCatalogAPI::with_client_and_config(dd_cfg, c),
        None => SoftwareCatalogAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_catalog_entity(ListCatalogEntityOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list catalog entities: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn entities_upsert(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SoftwareCatalogAPI::with_client_and_config(dd_cfg, c),
        None => SoftwareCatalogAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file::<UpsertCatalogEntityRequest>(file)?;
    let resp = api
        .upsert_catalog_entity(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to upsert catalog entity: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn entities_delete(cfg: &Config, entity_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SoftwareCatalogAPI::with_client_and_config(dd_cfg, c),
        None => SoftwareCatalogAPI::with_config(dd_cfg),
    };
    api.delete_catalog_entity(entity_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete catalog entity: {e:?}"))?;
    println!("Entity '{entity_id}' deleted successfully.");
    Ok(())
}

pub async fn kinds_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SoftwareCatalogAPI::with_client_and_config(dd_cfg, c),
        None => SoftwareCatalogAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_catalog_kind(ListCatalogKindOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list catalog kinds: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn kinds_upsert(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SoftwareCatalogAPI::with_client_and_config(dd_cfg, c),
        None => SoftwareCatalogAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file::<UpsertCatalogKindRequest>(file)?;
    let resp = api
        .upsert_catalog_kind(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to upsert catalog kind: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn kinds_delete(cfg: &Config, kind_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SoftwareCatalogAPI::with_client_and_config(dd_cfg, c),
        None => SoftwareCatalogAPI::with_config(dd_cfg),
    };
    api.delete_catalog_kind(kind_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete catalog kind: {e:?}"))?;
    println!("Kind '{kind_id}' deleted successfully.");
    Ok(())
}

pub async fn relations_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SoftwareCatalogAPI::with_client_and_config(dd_cfg, c),
        None => SoftwareCatalogAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_catalog_relation(ListCatalogRelationOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list catalog relations: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn entities_preview(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SoftwareCatalogAPI::with_client_and_config(dd_cfg, c),
        None => SoftwareCatalogAPI::with_config(dd_cfg),
    };
    let resp = api
        .preview_catalog_entities()
        .await
        .map_err(|e| anyhow::anyhow!("failed to preview catalog entities: {e:?}"))?;
    formatter::output(cfg, &resp)
}
