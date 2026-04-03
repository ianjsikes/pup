use anyhow::Result;
use datadog_api_client::datadogV1::api_service_level_objectives::{
    DeleteSLOOptionalParams, GetSLOOptionalParams, ListSLOsOptionalParams,
    ServiceLevelObjectivesAPI,
};
use datadog_api_client::datadogV1::model::{ServiceLevelObjective, ServiceLevelObjectiveRequest};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn list(
    cfg: &Config,
    query: Option<String>,
    tags_query: Option<String>,
    metrics_query: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceLevelObjectivesAPI::with_client_and_config(dd_cfg, c),
        None => ServiceLevelObjectivesAPI::with_config(dd_cfg),
    };
    let mut params = ListSLOsOptionalParams::default();
    if let Some(query) = query {
        params = params.query(query);
    }
    if let Some(tags_query) = tags_query {
        params = params.tags_query(tags_query);
    }
    if let Some(metrics_query) = metrics_query {
        params = params.metrics_query(metrics_query);
    }
    if let Some(limit) = limit {
        params = params.limit(limit);
    }
    if let Some(offset) = offset {
        params = params.offset(offset);
    }
    let resp = api
        .list_slos(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list SLOs: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn get(cfg: &Config, id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceLevelObjectivesAPI::with_client_and_config(dd_cfg, c),
        None => ServiceLevelObjectivesAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_slo(id.to_string(), GetSLOOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get SLO: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn create(cfg: &Config, file: &str) -> Result<()> {
    let body: ServiceLevelObjectiveRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceLevelObjectivesAPI::with_client_and_config(dd_cfg, c),
        None => ServiceLevelObjectivesAPI::with_config(dd_cfg),
    };
    let resp = api
        .create_slo(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create SLO: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn update(cfg: &Config, id: &str, file: &str) -> Result<()> {
    let body: ServiceLevelObjective = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceLevelObjectivesAPI::with_client_and_config(dd_cfg, c),
        None => ServiceLevelObjectivesAPI::with_config(dd_cfg),
    };
    let resp = api
        .update_slo(id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update SLO: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn delete(cfg: &Config, id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceLevelObjectivesAPI::with_client_and_config(dd_cfg, c),
        None => ServiceLevelObjectivesAPI::with_config(dd_cfg),
    };
    let resp = api
        .delete_slo(id.to_string(), DeleteSLOOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete SLO: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn status(cfg: &Config, id: &str, from_ts: i64, to_ts: i64) -> Result<()> {
    use datadog_api_client::datadogV2::api_service_level_objectives::{
        GetSloStatusOptionalParams, ServiceLevelObjectivesAPI as SloV2API,
    };

    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SloV2API::with_client_and_config(dd_cfg, c),
        None => SloV2API::with_config(dd_cfg),
    };
    let resp = api
        .get_slo_status(
            id.to_string(),
            from_ts,
            to_ts,
            GetSloStatusOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to get SLO status: {e:?}"))?;
    formatter::output(cfg, &resp)
}
