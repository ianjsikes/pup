use crate::client;
use crate::commands::ddsql;
use crate::config::Config;
use crate::formatter;
use crate::util;
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

const SCHEMA_URL: &str = "https://docs.datadoghq.com/security/guide/findings-schema.md";
const SCHEMA_SECTION_MARKER: &str = "## Schema Reference";

/// Fetch the security findings schema reference from Datadog docs.
///
/// Downloads the markdown page at runtime, extracts everything after
/// "## Schema Reference", and strips template directives ({% ... %})
/// so the output is clean, readable plaintext/markdown.
async fn fetch_schema_markdown() -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| anyhow::anyhow!("failed to create HTTP client: {e}"))?;
    let resp = client
        .get(SCHEMA_URL)
        .header("User-Agent", "pup-cli")
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("failed to fetch schema from {SCHEMA_URL}: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        anyhow::bail!("failed to fetch schema from {SCHEMA_URL} (HTTP {status})");
    }

    let body = resp
        .text()
        .await
        .map_err(|e| anyhow::anyhow!("failed to read schema response: {e}"))?;

    // Extract everything after "## Schema Reference"
    let schema_section = body
        .find(SCHEMA_SECTION_MARKER)
        .map(|pos| &body[pos..])
        .ok_or_else(|| {
            anyhow::anyhow!(
                "schema page did not contain expected section '{SCHEMA_SECTION_MARKER}'"
            )
        })?;

    // Trim trailing sections that aren't part of the schema reference
    let schema_section = schema_section
        .find("## Further reading")
        .map(|pos| schema_section[..pos].trim_end())
        .unwrap_or(schema_section);

    // Strip template directives: {% ... %}
    let mut cleaned = strip_template_directives(schema_section);

    // Add source attribution
    cleaned.push_str("\n\n---\n*This schema was fetched from Datadog public documentation.*\n");

    Ok(cleaned)
}

/// Remove template directives like {% collapsible-section %}, {% /collapsible-section %},
/// {% callout %}, {% /callout %}, {% tab %}, etc. Also removes lines that become empty
/// after stripping.
fn strip_template_directives(input: &str) -> String {
    let mut lines: Vec<&str> = Vec::new();
    for line in input.lines() {
        let trimmed = line.trim();
        // Skip lines that are entirely a template directive
        if trimmed.starts_with("{%") && trimmed.ends_with("%}") {
            continue;
        }
        // Strip inline template directives (e.g., "## Schema Reference{% #schema-reference %}")
        if let Some(pos) = line.find("{%") {
            let cleaned = line[..pos].trim_end();
            if !cleaned.is_empty() {
                lines.push(cleaned);
            }
        } else {
            lines.push(line);
        }
    }
    lines.join("\n")
}

pub async fn findings_schema(cfg: &Config) -> Result<()> {
    let schema = fetch_schema_markdown().await?;

    if cfg.agent_mode {
        eprintln!(
            "Use these fields with `pup security findings analyze --query \"SELECT ... FROM dd.security_findings(...)\"`"
        );
    }

    println!("{schema}");
    Ok(())
}

// ---- Findings Analyze ----

pub async fn findings_analyze(
    cfg: &Config,
    query: &str,
    from: &str,
    to: &str,
    limit: i64,
) -> Result<()> {
    if !query.contains("dd.security_findings") {
        eprintln!("Warning: query doesn't use dd.security_findings(). Did you mean to use `pup ddsql table`?");
    }

    match ddsql::execute_ddsql_query(cfg, query, from, to, Some(limit as i32)).await {
        Ok(rows) => formatter::output(cfg, &rows),
        Err(e) => {
            eprintln!(
                "Hint: run `pup security findings schema` to see available fields and types for dd.security_findings()."
            );
            Err(e)
        }
    }
}

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
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    let resp = api
        .list_application_security_waf_custom_rules()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list ASM WAF custom rules: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_custom_rules_get(cfg: &Config, custom_rule_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    let resp = api
        .get_application_security_waf_custom_rule(custom_rule_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get ASM WAF custom rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_custom_rules_create(cfg: &Config, file: &str) -> Result<()> {
    let body: ApplicationSecurityWafCustomRuleCreateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    let resp = api
        .create_application_security_waf_custom_rule(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create ASM WAF custom rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_custom_rules_update(cfg: &Config, custom_rule_id: &str, file: &str) -> Result<()> {
    let body: ApplicationSecurityWafCustomRuleUpdateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    let resp = api
        .update_application_security_waf_custom_rule(custom_rule_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update ASM WAF custom rule: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_custom_rules_delete(cfg: &Config, custom_rule_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    api.delete_application_security_waf_custom_rule(custom_rule_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete ASM WAF custom rule: {e:?}"))?;
    println!("ASM WAF custom rule '{custom_rule_id}' deleted.");
    Ok(())
}

// ---- ASM WAF Exclusion Filters ----

pub async fn asm_exclusions_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    let resp = api
        .list_application_security_waf_exclusion_filters()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list ASM WAF exclusion filters: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_exclusions_get(cfg: &Config, exclusion_filter_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    let resp = api
        .get_application_security_waf_exclusion_filter(exclusion_filter_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get ASM WAF exclusion filter: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_exclusions_create(cfg: &Config, file: &str) -> Result<()> {
    let body: ApplicationSecurityWafExclusionFilterCreateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
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
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    let resp = api
        .update_application_security_waf_exclusion_filter(exclusion_filter_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update ASM WAF exclusion filter: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn asm_exclusions_delete(cfg: &Config, exclusion_filter_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = ApplicationSecurityAPI::with_config(dd_cfg);
    api.delete_application_security_waf_exclusion_filter(exclusion_filter_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete ASM WAF exclusion filter: {e:?}"))?;
    println!("ASM WAF exclusion filter '{exclusion_filter_id}' deleted.");
    Ok(())
}

// ---- Restriction Policies ----

pub async fn restriction_policy_get(cfg: &Config, resource_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = RestrictionPoliciesAPI::with_config(dd_cfg);
    let resp = api
        .get_restriction_policy(resource_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get restriction policy: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn restriction_policy_update(cfg: &Config, resource_id: &str, file: &str) -> Result<()> {
    let body: RestrictionPolicyUpdateRequest = util::read_json_file(file)?;
    let dd_cfg = client::make_dd_config(cfg);
    let api = RestrictionPoliciesAPI::with_config(dd_cfg);
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
    let api = RestrictionPoliciesAPI::with_config(dd_cfg);
    api.delete_restriction_policy(resource_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete restriction policy: {e:?}"))?;
    println!("Restriction policy for '{resource_id}' deleted.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_template_directives_removes_standalone() {
        let input = "## Heading\n{% collapsible-section #foo %}\n### Sub\nContent here.\n{% /collapsible-section %}";
        let result = strip_template_directives(input);
        assert_eq!(result, "## Heading\n### Sub\nContent here.");
    }

    #[test]
    fn test_strip_template_directives_removes_inline() {
        let input = "## Schema Reference{% #schema-reference %}\nSome text.";
        let result = strip_template_directives(input);
        assert_eq!(result, "## Schema Reference\nSome text.");
    }

    #[test]
    fn test_strip_template_directives_preserves_tables() {
        let input = "| Name | Type |\n| ---- | ---- |\n| `severity` | string |";
        let result = strip_template_directives(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_strip_template_directives_empty_input() {
        assert_eq!(strip_template_directives(""), "");
    }

    #[test]
    fn test_strip_template_directives_mixed() {
        let input = "Line 1\n{% tab title=\"Foo\" %}\nLine 2\n{% /tab %}\nLine 3";
        let result = strip_template_directives(input);
        assert_eq!(result, "Line 1\nLine 2\nLine 3");
    }
}
