use anyhow::Result;
use datadog_api_client::datadogV2::api_cloud_cost_management::CloudCostManagementAPI;
use datadog_api_client::datadogV2::api_usage_metering::{
    GetCostByOrgOptionalParams, GetMonthlyCostAttributionOptionalParams,
    GetProjectedCostOptionalParams, UsageMeteringAPI as UsageMeteringV2API,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn make_usage_api(cfg: &Config) -> UsageMeteringV2API {
    // Cost/billing endpoints require API key auth — no OAuth support.
    UsageMeteringV2API::with_config(client::make_dd_config(cfg))
}

pub async fn projected(cfg: &Config) -> Result<()> {
    let api = make_usage_api(cfg);
    let resp = api
        .get_projected_cost(GetProjectedCostOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get projected cost: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn by_org(cfg: &Config, start_month: String, end_month: Option<String>) -> Result<()> {
    let api = make_usage_api(cfg);

    let start_dt =
        chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&start_month)?)
            .unwrap();

    let mut params = GetCostByOrgOptionalParams::default();
    if let Some(e) = end_month {
        let end_dt =
            chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&e)?).unwrap();
        params = params.end_month(end_dt);
    }

    let resp = api
        .get_cost_by_org(start_dt, params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get cost by org: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn attribution(cfg: &Config, start: String, fields: Option<String>) -> Result<()> {
    let api = make_usage_api(cfg);

    let start_dt =
        chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&start)?).unwrap();

    let fields_str = fields.unwrap_or_else(|| "*".to_string());
    let params = GetMonthlyCostAttributionOptionalParams::default();

    let resp = api
        .get_monthly_cost_attribution(start_dt, fields_str, params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get cost attribution: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- Cloud Cost Management — AWS CUR Config ----

fn make_ccm_api(cfg: &Config) -> CloudCostManagementAPI {
    // CCM endpoints require API key auth — no OAuth support.
    CloudCostManagementAPI::with_config(client::make_dd_config(cfg))
}

pub async fn aws_config_list(cfg: &Config) -> Result<()> {
    let api = make_ccm_api(cfg);
    let resp = api
        .list_cost_awscur_configs()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list AWS CUR configs: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn aws_config_get(cfg: &Config, id: i64) -> Result<()> {
    let api = make_ccm_api(cfg);
    let resp = api
        .get_cost_awscur_config(id)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get AWS CUR config: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn aws_config_create(cfg: &Config, file: &str) -> Result<()> {
    let body: datadog_api_client::datadogV2::model::AwsCURConfigPostRequest =
        util::read_json_file(file)?;
    let api = make_ccm_api(cfg);
    let resp = api
        .create_cost_awscur_config(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create AWS CUR config: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn aws_config_delete(cfg: &Config, id: i64) -> Result<()> {
    let api = make_ccm_api(cfg);
    api.delete_cost_awscur_config(id)
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete AWS CUR config: {e:?}"))?;
    eprintln!("AWS CUR config {id} deleted.");
    Ok(())
}

// ---- Cloud Cost Management — Azure UC Config ----

pub async fn azure_config_list(cfg: &Config) -> Result<()> {
    let api = make_ccm_api(cfg);
    let resp = api
        .list_cost_azure_uc_configs()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list Azure UC configs: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn azure_config_get(cfg: &Config, id: i64) -> Result<()> {
    let api = make_ccm_api(cfg);
    let resp = api
        .get_cost_azure_uc_config(id)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get Azure UC config: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn azure_config_create(cfg: &Config, file: &str) -> Result<()> {
    let body: datadog_api_client::datadogV2::model::AzureUCConfigPostRequest =
        util::read_json_file(file)?;
    let api = make_ccm_api(cfg);
    let resp = api
        .create_cost_azure_uc_configs(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create Azure UC config: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn azure_config_delete(cfg: &Config, id: i64) -> Result<()> {
    let api = make_ccm_api(cfg);
    api.delete_cost_azure_uc_config(id)
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete Azure UC config: {e:?}"))?;
    eprintln!("Azure UC config {id} deleted.");
    Ok(())
}

// ---- Cloud Cost Management — GCP Usage Cost Config ----

pub async fn gcp_config_list(cfg: &Config) -> Result<()> {
    let api = make_ccm_api(cfg);
    let resp = api
        .list_cost_gcp_usage_cost_configs()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list GCP usage cost configs: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn gcp_config_get(cfg: &Config, id: i64) -> Result<()> {
    let api = make_ccm_api(cfg);
    let resp = api
        .get_cost_gcp_usage_cost_config(id)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get GCP usage cost config: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn gcp_config_create(cfg: &Config, file: &str) -> Result<()> {
    let body: datadog_api_client::datadogV2::model::GCPUsageCostConfigPostRequest =
        util::read_json_file(file)?;
    let api = make_ccm_api(cfg);
    let resp = api
        .create_cost_gcp_usage_cost_config(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create GCP usage cost config: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn gcp_config_delete(cfg: &Config, id: i64) -> Result<()> {
    let api = make_ccm_api(cfg);
    api.delete_cost_gcp_usage_cost_config(id)
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete GCP usage cost config: {e:?}"))?;
    eprintln!("GCP usage cost config {id} deleted.");
    Ok(())
}
