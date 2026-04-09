use anyhow::Result;
use datadog_api_client::datadogV2::api_static_analysis::{
    ListCustomRuleRevisionsOptionalParams, StaticAnalysisAPI,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

// ---------------------------------------------------------------------------
// Custom rulesets
// ---------------------------------------------------------------------------

pub async fn custom_rulesets_get(cfg: &Config, id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => StaticAnalysisAPI::with_client_and_config(dd_cfg, c),
        None => StaticAnalysisAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_custom_ruleset(id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get custom ruleset: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn custom_rulesets_update(cfg: &Config, ruleset_name: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => StaticAnalysisAPI::with_client_and_config(dd_cfg, c),
        None => StaticAnalysisAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .update_custom_ruleset(ruleset_name.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update custom ruleset: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn custom_rulesets_delete(cfg: &Config, ruleset_name: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => StaticAnalysisAPI::with_client_and_config(dd_cfg, c),
        None => StaticAnalysisAPI::with_config(dd_cfg),
    };
    api.delete_custom_ruleset(ruleset_name.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete custom ruleset: {e:?}"))?;
    println!("Custom ruleset '{ruleset_name}' deleted successfully.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Custom rules within a ruleset
// ---------------------------------------------------------------------------

pub async fn custom_rules_get(cfg: &Config, ruleset_name: &str, rule_name: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => StaticAnalysisAPI::with_client_and_config(dd_cfg, c),
        None => StaticAnalysisAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_custom_rule(ruleset_name.to_string(), rule_name.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get custom rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn custom_rules_create(cfg: &Config, ruleset_name: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => StaticAnalysisAPI::with_client_and_config(dd_cfg, c),
        None => StaticAnalysisAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_custom_rule(ruleset_name.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create custom rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn custom_rules_delete(cfg: &Config, ruleset_name: &str, rule_name: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => StaticAnalysisAPI::with_client_and_config(dd_cfg, c),
        None => StaticAnalysisAPI::with_config(dd_cfg),
    };
    api.delete_custom_rule(ruleset_name.to_string(), rule_name.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete custom rule: {e:?}"))?;
    println!("Custom rule '{rule_name}' in ruleset '{ruleset_name}' deleted successfully.");
    Ok(())
}

pub async fn custom_rule_revisions_list(
    cfg: &Config,
    ruleset_name: &str,
    rule_name: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => StaticAnalysisAPI::with_client_and_config(dd_cfg, c),
        None => StaticAnalysisAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_custom_rule_revisions(
            ruleset_name.to_string(),
            rule_name.to_string(),
            ListCustomRuleRevisionsOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to list custom rule revisions: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn custom_rule_revision_get(
    cfg: &Config,
    ruleset_name: &str,
    rule_name: &str,
    revision_id: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => StaticAnalysisAPI::with_client_and_config(dd_cfg, c),
        None => StaticAnalysisAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_custom_rule_revision(
            ruleset_name.to_string(),
            rule_name.to_string(),
            revision_id.to_string(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to get custom rule revision: {e:?}"))?;
    formatter::output(cfg, &resp)
}
