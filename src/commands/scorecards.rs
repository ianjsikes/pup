use anyhow::Result;
use datadog_api_client::datadogV2::api_service_scorecards::{
    ListScorecardOutcomesOptionalParams, ListScorecardRulesOptionalParams, ServiceScorecardsAPI,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn rules_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceScorecardsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceScorecardsAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_scorecard_rules(ListScorecardRulesOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list scorecard rules: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn outcomes_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceScorecardsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceScorecardsAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_scorecard_outcomes(ListScorecardOutcomesOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list scorecard outcomes: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn rules_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceScorecardsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceScorecardsAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_scorecard_rule(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create scorecard rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn rules_update(cfg: &Config, rule_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceScorecardsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceScorecardsAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .update_scorecard_rule(rule_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update scorecard rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn rules_delete(cfg: &Config, rule_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceScorecardsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceScorecardsAPI::with_config(dd_cfg),
    };
    api.delete_scorecard_rule(rule_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete scorecard rule: {e:?}"))?;
    println!("Scorecard rule '{rule_id}' deleted successfully.");
    Ok(())
}

pub async fn outcomes_batch_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceScorecardsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceScorecardsAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_scorecard_outcomes_batch(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create scorecard outcomes batch: {e:?}"))?;
    formatter::output(cfg, &resp)
}
