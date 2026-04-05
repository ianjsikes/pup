use anyhow::Result;
use datadog_api_client::datadogV2::api_application_security::ApplicationSecurityAPI;
use datadog_api_client::datadogV2::api_entity_risk_scores::{
    EntityRiskScoresAPI, ListEntityRiskScoresOptionalParams,
};
use datadog_api_client::datadogV2::api_restriction_policies::{
    RestrictionPoliciesAPI, UpdateRestrictionPolicyOptionalParams,
};
use datadog_api_client::datadogV2::api_security_monitoring::{
    ListFindingsOptionalParams, ListSecurityMonitoringRulesOptionalParams,
    ListSecurityMonitoringSuppressionsOptionalParams,
    SearchSecurityMonitoringSignalsOptionalParams, SecurityMonitoringAPI,
};
use datadog_api_client::datadogV2::model::{
    ApplicationSecurityWafCustomRuleCreateRequest, ApplicationSecurityWafCustomRuleUpdateRequest,
    ApplicationSecurityWafExclusionFilterCreateRequest,
    ApplicationSecurityWafExclusionFilterUpdateRequest, RestrictionPolicyUpdateRequest,
    SecurityMonitoringRuleBulkExportAttributes, SecurityMonitoringRuleBulkExportData,
    SecurityMonitoringRuleBulkExportDataType, SecurityMonitoringRuleBulkExportPayload,
    SecurityMonitoringRuleSort, SecurityMonitoringSignalListRequest,
    SecurityMonitoringSignalListRequestFilter, SecurityMonitoringSignalListRequestPage,
    SecurityMonitoringSignalsSort, SecurityMonitoringSuppressionCreateRequest,
    SecurityMonitoringSuppressionSort, SecurityMonitoringSuppressionUpdateRequest,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn rules_list(cfg: &Config, sort: Option<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let mut params = ListSecurityMonitoringRulesOptionalParams::default();
    if let Some(s) = sort {
        params = params.sort(parse_rule_sort(&s));
    }
    let resp = api
        .list_security_monitoring_rules(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list rules: {e:?}"))?;
    formatter::output(cfg, &resp)
}

fn parse_rule_sort(s: &str) -> SecurityMonitoringRuleSort {
    match s {
        "name" => SecurityMonitoringRuleSort::NAME,
        "-name" => SecurityMonitoringRuleSort::NAME_DESCENDING,
        "creation_date" => SecurityMonitoringRuleSort::CREATION_DATE,
        "-creation_date" => SecurityMonitoringRuleSort::CREATION_DATE_DESCENDING,
        "update_date" => SecurityMonitoringRuleSort::UPDATE_DATE,
        "-update_date" => SecurityMonitoringRuleSort::UPDATE_DATE_DESCENDING,
        "enabled" => SecurityMonitoringRuleSort::ENABLED,
        "-enabled" => SecurityMonitoringRuleSort::ENABLED_DESCENDING,
        "type" => SecurityMonitoringRuleSort::TYPE,
        "-type" => SecurityMonitoringRuleSort::TYPE_DESCENDING,
        "highest_severity" => SecurityMonitoringRuleSort::HIGHEST_SEVERITY,
        "-highest_severity" => SecurityMonitoringRuleSort::HIGHEST_SEVERITY_DESCENDING,
        "source" => SecurityMonitoringRuleSort::SOURCE,
        "-source" => SecurityMonitoringRuleSort::SOURCE_DESCENDING,
        _ => SecurityMonitoringRuleSort::NAME,
    }
}

pub async fn rules_get(cfg: &Config, rule_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_security_monitoring_rule(rule_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn signals_search(
    cfg: &Config,
    query: String,
    from: String,
    to: String,
    limit: i32,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };

    let from_dt =
        chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&from)?).unwrap();
    let to_dt =
        chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&to)?).unwrap();

    let body = SecurityMonitoringSignalListRequest::new()
        .filter(
            SecurityMonitoringSignalListRequestFilter::new()
                .query(query)
                .from(from_dt)
                .to(to_dt),
        )
        .page(SecurityMonitoringSignalListRequestPage::new().limit(limit))
        .sort(SecurityMonitoringSignalsSort::TIMESTAMP_DESCENDING);

    let params = SearchSecurityMonitoringSignalsOptionalParams::default().body(body);
    let resp = api
        .search_security_monitoring_signals(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to search signals: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn findings_search(cfg: &Config, query: Option<String>, limit: i64) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let mut params = ListFindingsOptionalParams::default().page_limit(limit);
    if let Some(q) = query {
        params = params.filter_tags(q);
    }
    let resp = api
        .list_findings(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to search findings: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- Bulk Export ----

pub async fn rules_bulk_export(cfg: &Config, rule_ids: Vec<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let attrs = SecurityMonitoringRuleBulkExportAttributes::new(rule_ids);
    let data = SecurityMonitoringRuleBulkExportData::new(
        attrs,
        SecurityMonitoringRuleBulkExportDataType::SECURITY_MONITORING_RULES_BULK_EXPORT,
    );
    let body = SecurityMonitoringRuleBulkExportPayload::new(data);
    let resp = api
        .bulk_export_security_monitoring_rules(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to bulk export security rules: {e:?}"))?;
    // resp is Vec<u8> (ZIP data), output as raw bytes to stdout
    let output = String::from_utf8_lossy(&resp);
    println!("{output}");
    Ok(())
}

// ---- Content Packs ----

pub async fn content_packs_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_content_packs_states()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list content packs: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn content_packs_activate(cfg: &Config, pack_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    api.activate_content_pack(pack_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to activate content pack: {e:?}"))?;
    println!("Content pack '{pack_id}' activated successfully.");
    Ok(())
}

pub async fn content_packs_deactivate(cfg: &Config, pack_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    api.deactivate_content_pack(pack_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to deactivate content pack: {e:?}"))?;
    println!("Content pack '{pack_id}' deactivated successfully.");
    Ok(())
}

// ---- Risk Scores ----

pub async fn risk_scores_list(cfg: &Config, query: Option<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => EntityRiskScoresAPI::with_client_and_config(dd_cfg, c),
        None => EntityRiskScoresAPI::with_config(dd_cfg),
    };
    let mut params = ListEntityRiskScoresOptionalParams::default();
    if let Some(q) = query {
        params = params.filter_query(q);
    }
    let resp = api
        .list_entity_risk_scores(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list entity risk scores: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- Suppressions ----

fn parse_suppression_sort(s: &str) -> SecurityMonitoringSuppressionSort {
    match s {
        "name" => SecurityMonitoringSuppressionSort::NAME,
        "-name" => SecurityMonitoringSuppressionSort::NAME_DESCENDING,
        "start_date" => SecurityMonitoringSuppressionSort::START_DATE,
        "-start_date" => SecurityMonitoringSuppressionSort::START_DATE_DESCENDING,
        "expiration_date" => SecurityMonitoringSuppressionSort::EXPIRATION_DATE,
        "-expiration_date" => SecurityMonitoringSuppressionSort::EXPIRATION_DATE_DESCENDING,
        "update_date" => SecurityMonitoringSuppressionSort::UPDATE_DATE,
        "-update_date" => SecurityMonitoringSuppressionSort::UPDATE_DATE_DESCENDING,
        "-creation_date" => SecurityMonitoringSuppressionSort::CREATION_DATE_DESCENDING,
        "enabled" => SecurityMonitoringSuppressionSort::ENABLED,
        "-enabled" => SecurityMonitoringSuppressionSort::ENABLED_DESCENDING,
        _ => SecurityMonitoringSuppressionSort::NAME,
    }
}

pub async fn suppressions_list(cfg: &Config, sort: Option<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let mut params = ListSecurityMonitoringSuppressionsOptionalParams::default();
    if let Some(s) = sort {
        params = params.sort(parse_suppression_sort(&s));
    }
    let resp = api
        .list_security_monitoring_suppressions(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list suppressions: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn suppressions_get(cfg: &Config, suppression_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_security_monitoring_suppression(suppression_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get suppression: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn suppressions_create(cfg: &Config, file: &str) -> Result<()> {
    let body: SecurityMonitoringSuppressionCreateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let resp = api
        .create_security_monitoring_suppression(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create suppression: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn suppressions_update(cfg: &Config, suppression_id: &str, file: &str) -> Result<()> {
    let body: SecurityMonitoringSuppressionUpdateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    let resp = api
        .update_security_monitoring_suppression(suppression_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update suppression: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn suppressions_delete(cfg: &Config, suppression_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    api.delete_security_monitoring_suppression(suppression_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete suppression: {e:?}"))?;
    println!("Suppression '{suppression_id}' deleted.");
    Ok(())
}

pub async fn suppressions_validate(cfg: &Config, file: &str) -> Result<()> {
    let body: SecurityMonitoringSuppressionCreateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => SecurityMonitoringAPI::with_client_and_config(dd_cfg, c),
        None => SecurityMonitoringAPI::with_config(dd_cfg),
    };
    api.validate_security_monitoring_suppression(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to validate suppression: {e:?}"))?;
    println!("Suppression is valid.");
    Ok(())
}

// ---- ASM WAF Custom Rules ----

pub async fn asm_custom_rules_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_application_security_waf_custom_rules()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list ASM WAF custom rules: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_custom_rules_get(cfg: &Config, custom_rule_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_application_security_waf_custom_rule(custom_rule_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get ASM WAF custom rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_custom_rules_create(cfg: &Config, file: &str) -> Result<()> {
    let body: ApplicationSecurityWafCustomRuleCreateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    let resp = api
        .create_application_security_waf_custom_rule(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create ASM WAF custom rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_custom_rules_update(cfg: &Config, custom_rule_id: &str, file: &str) -> Result<()> {
    let body: ApplicationSecurityWafCustomRuleUpdateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    let resp = api
        .update_application_security_waf_custom_rule(custom_rule_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update ASM WAF custom rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_custom_rules_delete(cfg: &Config, custom_rule_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    api.delete_application_security_waf_custom_rule(custom_rule_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete ASM WAF custom rule: {e:?}"))?;
    println!("ASM WAF custom rule '{custom_rule_id}' deleted.");
    Ok(())
}

// ---- ASM WAF Exclusion Filters ----

pub async fn asm_exclusions_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_application_security_waf_exclusion_filters()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list ASM WAF exclusion filters: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_exclusions_get(cfg: &Config, exclusion_filter_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_application_security_waf_exclusion_filter(exclusion_filter_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get ASM WAF exclusion filter: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_exclusions_create(cfg: &Config, file: &str) -> Result<()> {
    let body: ApplicationSecurityWafExclusionFilterCreateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    let resp = api
        .create_application_security_waf_exclusion_filter(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create ASM WAF exclusion filter: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_exclusions_update(
    cfg: &Config,
    exclusion_filter_id: &str,
    file: &str,
) -> Result<()> {
    let body: ApplicationSecurityWafExclusionFilterUpdateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    let resp = api
        .update_application_security_waf_exclusion_filter(exclusion_filter_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update ASM WAF exclusion filter: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_exclusions_delete(cfg: &Config, exclusion_filter_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ApplicationSecurityAPI::with_client_and_config(dd_cfg, c),
        None => ApplicationSecurityAPI::with_config(dd_cfg),
    };
    api.delete_application_security_waf_exclusion_filter(exclusion_filter_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete ASM WAF exclusion filter: {e:?}"))?;
    println!("ASM WAF exclusion filter '{exclusion_filter_id}' deleted.");
    Ok(())
}

// ---- Restriction Policies ----

pub async fn restriction_policy_get(cfg: &Config, resource_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RestrictionPoliciesAPI::with_client_and_config(dd_cfg, c),
        None => RestrictionPoliciesAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_restriction_policy(resource_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get restriction policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn restriction_policy_update(cfg: &Config, resource_id: &str, file: &str) -> Result<()> {
    let body: RestrictionPolicyUpdateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RestrictionPoliciesAPI::with_client_and_config(dd_cfg, c),
        None => RestrictionPoliciesAPI::with_config(dd_cfg),
    };
    let resp = api
        .update_restriction_policy(
            resource_id.to_string(),
            body,
            UpdateRestrictionPolicyOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to update restriction policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn restriction_policy_delete(cfg: &Config, resource_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RestrictionPoliciesAPI::with_client_and_config(dd_cfg, c),
        None => RestrictionPoliciesAPI::with_config(dd_cfg),
    };
    api.delete_restriction_policy(resource_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete restriction policy: {e:?}"))?;
    println!("Restriction policy for '{resource_id}' deleted.");
    Ok(())
}
