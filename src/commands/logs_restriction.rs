use anyhow::Result;
use datadog_api_client::datadogV2::api_logs_restriction_queries::{
    ListRestrictionQueriesOptionalParams, ListRestrictionQueryRolesOptionalParams,
    LogsRestrictionQueriesAPI,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => LogsRestrictionQueriesAPI::with_client_and_config(dd_cfg, c),
        None => LogsRestrictionQueriesAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_restriction_queries(ListRestrictionQueriesOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list restriction queries: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn get(cfg: &Config, query_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => LogsRestrictionQueriesAPI::with_client_and_config(dd_cfg, c),
        None => LogsRestrictionQueriesAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_restriction_query(query_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get restriction query: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => LogsRestrictionQueriesAPI::with_client_and_config(dd_cfg, c),
        None => LogsRestrictionQueriesAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_restriction_query(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create restriction query: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn update(cfg: &Config, query_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => LogsRestrictionQueriesAPI::with_client_and_config(dd_cfg, c),
        None => LogsRestrictionQueriesAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .update_restriction_query(query_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update restriction query: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn delete(cfg: &Config, query_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => LogsRestrictionQueriesAPI::with_client_and_config(dd_cfg, c),
        None => LogsRestrictionQueriesAPI::with_config(dd_cfg),
    };
    api.delete_restriction_query(query_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete restriction query: {e:?}"))?;
    println!("Restriction query '{query_id}' deleted successfully.");
    Ok(())
}

pub async fn roles_list(cfg: &Config, query_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => LogsRestrictionQueriesAPI::with_client_and_config(dd_cfg, c),
        None => LogsRestrictionQueriesAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_restriction_query_roles(
            query_id.to_string(),
            ListRestrictionQueryRolesOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to list restriction query roles: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn roles_add(cfg: &Config, query_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => LogsRestrictionQueriesAPI::with_client_and_config(dd_cfg, c),
        None => LogsRestrictionQueriesAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    api.add_role_to_restriction_query(query_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to add role to restriction query: {e:?}"))?;
    println!("Role added to restriction query '{query_id}'.");
    Ok(())
}
