use anyhow::Result;
use datadog_api_client::datadogV2::api_csm_threats::{
    CSMThreatsAPI, DeleteCSMThreatsAgentRuleOptionalParams, GetCSMThreatsAgentRuleOptionalParams,
    ListCSMThreatsAgentRulesOptionalParams, UpdateCSMThreatsAgentRuleOptionalParams,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn agent_policies_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
    let resp = api
        .list_csm_threats_agent_policies()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list CSM threats agent policies: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_policies_get(cfg: &Config, policy_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
    let resp = api
        .get_csm_threats_agent_policy(policy_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get CSM threats agent policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_policies_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
    let body = util::read_json_file(file)?;
    let resp = api
        .create_csm_threats_agent_policy(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create CSM threats agent policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_policies_update(cfg: &Config, policy_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
    let body = util::read_json_file(file)?;
    let resp = api
        .update_csm_threats_agent_policy(policy_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update CSM threats agent policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn agent_policies_delete(cfg: &Config, policy_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
    api.delete_csm_threats_agent_policy(policy_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete CSM threats agent policy: {e:?}"))?;
    println!("Agent policy '{policy_id}' deleted successfully.");
    Ok(())
}

pub async fn agent_rules_list(cfg: &Config, policy_id: Option<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
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
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
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
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
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
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
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
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
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
    let dd_cfg = client::make_dd_config(cfg);
    let api = CSMThreatsAPI::with_config(dd_cfg);
    let bytes = api
        .download_csm_threats_policy()
        .await
        .map_err(|e| anyhow::anyhow!("failed to download CSM threats policy: {e:?}"))?;
    let content = String::from_utf8_lossy(&bytes);
    println!("{content}");
    Ok(())
}
