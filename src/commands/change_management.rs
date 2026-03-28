use anyhow::Result;
use datadog_api_client::datadogV2::api_change_management::ChangeManagementAPI;
use datadog_api_client::datadogV2::model::{
    ChangeRequestBranchCreateRequest, ChangeRequestCreateRequest,
    ChangeRequestDecisionUpdateRequest, ChangeRequestUpdateRequest,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn make_api(cfg: &Config) -> ChangeManagementAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => ChangeManagementAPI::with_client_and_config(dd_cfg, c),
        None => ChangeManagementAPI::with_config(dd_cfg),
    }
}

pub async fn create(cfg: &Config, file: &str) -> Result<()> {
    let body: ChangeRequestCreateRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_change_request(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create change request: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn get(cfg: &Config, change_request_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_change_request(change_request_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get change request: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn update(cfg: &Config, change_request_id: &str, file: &str) -> Result<()> {
    let body: ChangeRequestUpdateRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_change_request(change_request_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update change request: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn create_branch(cfg: &Config, change_request_id: &str, file: &str) -> Result<()> {
    let body: ChangeRequestBranchCreateRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_change_request_branch(change_request_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create change request branch: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn delete_decision(
    cfg: &Config,
    change_request_id: &str,
    decision_id: &str,
) -> Result<()> {
    let api = make_api(cfg);
    api.delete_change_request_decision(change_request_id.to_string(), decision_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete change request decision: {e:?}"))?;
    println!("Decision '{decision_id}' deleted.");
    Ok(())
}

pub async fn update_decision(
    cfg: &Config,
    change_request_id: &str,
    decision_id: &str,
    file: &str,
) -> Result<()> {
    let body: ChangeRequestDecisionUpdateRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_change_request_decision(
            change_request_id.to_string(),
            decision_id.to_string(),
            body,
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to update change request decision: {e:?}"))?;
    formatter::output(cfg, &resp)
}
