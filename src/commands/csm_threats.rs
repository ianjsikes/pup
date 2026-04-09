use anyhow::Result;
use datadog_api_client::datadogV2::api_csm_threats::{
    CSMThreatsAPI, DeleteCSMThreatsAgentRuleOptionalParams, GetCSMThreatsAgentRuleOptionalParams,
    ListCSMThreatsAgentRulesOptionalParams, UpdateCSMThreatsAgentRuleOptionalParams,
};
use datadog_api_client::datadogV2::api_security_monitoring::{
    ListSecurityMonitoringRulesOptionalParams, SecurityMonitoringAPI,
};
use datadog_api_client::datadogV2::model::{
    SecurityMonitoringRuleCreatePayload, SecurityMonitoringRuleUpdatePayload,
    SecurityMonitoringRuleValidatePayload,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn make_csm_api(cfg: &Config) -> CSMThreatsAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => CSMThreatsAPI::with_client_and_config(dd_cfg, c),
        None => CSMThreatsAPI::with_config(dd_cfg),
    }
}

pub async fn agent_policies_list(cfg: &Config) -> Result<()> {
    let api = make_csm_api(cfg);
    let resp = api
        .list_csm_threats_agent_policies()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list CSM threats agent policies: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_policies_get(cfg: &Config, policy_id: &str) -> Result<()> {
    let api = make_csm_api(cfg);
    let resp = api
        .get_csm_threats_agent_policy(policy_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get CSM threats agent policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_policies_create(cfg: &Config, file: &str) -> Result<()> {
    let api = make_csm_api(cfg);
    let body = util::read_json_file(file)?;
    let resp = api
        .create_csm_threats_agent_policy(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create CSM threats agent policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_policies_update(cfg: &Config, policy_id: &str, file: &str) -> Result<()> {
    let api = make_csm_api(cfg);
    let body = util::read_json_file(file)?;
    let resp = api
        .update_csm_threats_agent_policy(policy_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update CSM threats agent policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_policies_delete(cfg: &Config, policy_id: &str) -> Result<()> {
    let api = make_csm_api(cfg);
    api.delete_csm_threats_agent_policy(policy_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete CSM threats agent policy: {e:?}"))?;
    println!("Agent policy '{policy_id}' deleted successfully.");
    Ok(())
}

pub async fn agent_rules_list(cfg: &Config, policy_id: Option<String>) -> Result<()> {
    let api = make_csm_api(cfg);
    let mut params = ListCSMThreatsAgentRulesOptionalParams::default();
    if let Some(pid) = policy_id {
        params = params.policy_id(pid);
    }
    let resp = api
        .list_csm_threats_agent_rules(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list CSM threats agent rules: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_rules_get(cfg: &Config, rule_id: &str, policy_id: Option<String>) -> Result<()> {
    let api = make_csm_api(cfg);
    let mut params = GetCSMThreatsAgentRuleOptionalParams::default();
    if let Some(pid) = policy_id {
        params = params.policy_id(pid);
    }
    let resp = api
        .get_csm_threats_agent_rule(rule_id.to_string(), params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get CSM threats agent rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_rules_create(cfg: &Config, file: &str) -> Result<()> {
    let api = make_csm_api(cfg);
    let body = util::read_json_file(file)?;
    let resp = api
        .create_csm_threats_agent_rule(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create CSM threats agent rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_rules_update(
    cfg: &Config,
    rule_id: &str,
    file: &str,
    policy_id: Option<String>,
) -> Result<()> {
    let api = make_csm_api(cfg);
    let body = util::read_json_file(file)?;
    let mut params = UpdateCSMThreatsAgentRuleOptionalParams::default();
    if let Some(pid) = policy_id {
        params = params.policy_id(pid);
    }
    let resp = api
        .update_csm_threats_agent_rule(rule_id.to_string(), body, params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update CSM threats agent rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_rules_delete(
    cfg: &Config,
    rule_id: &str,
    policy_id: Option<String>,
) -> Result<()> {
    let api = make_csm_api(cfg);
    let mut params = DeleteCSMThreatsAgentRuleOptionalParams::default();
    if let Some(pid) = policy_id {
        params = params.policy_id(pid);
    }
    api.delete_csm_threats_agent_rule(rule_id.to_string(), params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete CSM threats agent rule: {e:?}"))?;
    println!("Agent rule '{rule_id}' deleted successfully.");
    Ok(())
}

pub async fn policy_download(cfg: &Config) -> Result<()> {
    let api = make_csm_api(cfg);
    let bytes = api
        .download_csm_threats_policy()
        .await
        .map_err(|e| anyhow::anyhow!("failed to download CSM threats policy: {e:?}"))?;
    let content = String::from_utf8_lossy(&bytes);
    println!("{content}");
    Ok(())
}

// ---- Backend Rules (Workload Security detection rules) ----

fn make_sec_api(cfg: &Config) -> SecurityMonitoringAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    }
}

pub async fn backend_rules_list(cfg: &Config, query: Option<String>) -> Result<()> {
    let api = make_sec_api(cfg);
    let filter = match query {
        Some(q) => format!("type:workload_security {q}"),
        None => "type:workload_security".to_string(),
    };
    let params = ListSecurityMonitoringRulesOptionalParams::default()
        .page_size(100)
        .query(filter);
    let resp = api
        .list_security_monitoring_rules(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list backend rules: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn backend_rules_get(cfg: &Config, rule_id: &str) -> Result<()> {
    let api = make_sec_api(cfg);
    let resp = api
        .get_security_monitoring_rule(rule_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get backend rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn backend_rules_create(cfg: &Config, file: &str) -> Result<()> {
    let body: SecurityMonitoringRuleCreatePayload = util::read_json_file(file)?;
    let api = make_sec_api(cfg);
    let resp = api
        .create_security_monitoring_rule(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create backend rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn backend_rules_update(cfg: &Config, rule_id: &str, file: &str) -> Result<()> {
    let body: SecurityMonitoringRuleUpdatePayload = util::read_json_file(file)?;
    let api = make_sec_api(cfg);
    let resp = api
        .update_security_monitoring_rule(rule_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update backend rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn backend_rules_delete(cfg: &Config, rule_id: &str) -> Result<()> {
    let api = make_sec_api(cfg);
    api.delete_security_monitoring_rule(rule_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete backend rule: {e:?}"))?;
    println!("Backend rule '{rule_id}' deleted successfully.");
    Ok(())
}

pub async fn backend_rules_validate(cfg: &Config, file: &str) -> Result<()> {
    let body: SecurityMonitoringRuleValidatePayload = util::read_json_file(file)?;
    let api = make_sec_api(cfg);
    api.validate_security_monitoring_rule(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to validate backend rule: {e:?}"))?;
    println!("Backend rule is valid.");
    Ok(())
}
