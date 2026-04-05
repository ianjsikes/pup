use anyhow::Result;
use datadog_api_client::datadogV2::api_agentless_scanning::AgentlessScanningAPI;

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn aws_scan_options_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_aws_scan_options()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list AWS scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn aws_scan_options_get(cfg: &Config, account_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_aws_scan_options(account_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get AWS scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn aws_scan_options_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_aws_scan_options(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create AWS scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn aws_scan_options_update(cfg: &Config, account_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    api.update_aws_scan_options(account_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update AWS scan options: {e:?}"))?;
    println!("AWS scan options for account '{account_id}' updated successfully.");
    Ok(())
}

pub async fn aws_scan_options_delete(cfg: &Config, account_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    api.delete_aws_scan_options(account_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete AWS scan options: {e:?}"))?;
    println!("AWS scan options for account '{account_id}' deleted successfully.");
    Ok(())
}

pub async fn aws_on_demand_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_aws_on_demand_tasks()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list AWS on-demand tasks: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn aws_on_demand_get(cfg: &Config, task_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_aws_on_demand_task(task_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get AWS on-demand task: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn aws_on_demand_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_aws_on_demand_task(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create AWS on-demand task: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn azure_scan_options_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_azure_scan_options()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list Azure scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn azure_scan_options_get(cfg: &Config, subscription_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_azure_scan_options(subscription_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get Azure scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn azure_scan_options_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_azure_scan_options(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create Azure scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn azure_scan_options_update(
    cfg: &Config,
    subscription_id: &str,
    file: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .update_azure_scan_options(subscription_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update Azure scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn azure_scan_options_delete(cfg: &Config, subscription_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    api.delete_azure_scan_options(subscription_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete Azure scan options: {e:?}"))?;
    println!("Azure scan options for subscription '{subscription_id}' deleted successfully.");
    Ok(())
}

pub async fn gcp_scan_options_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_gcp_scan_options()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list GCP scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn gcp_scan_options_get(cfg: &Config, project_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_gcp_scan_options(project_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get GCP scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn gcp_scan_options_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_gcp_scan_options(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create GCP scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn gcp_scan_options_update(cfg: &Config, project_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .update_gcp_scan_options(project_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update GCP scan options: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn gcp_scan_options_delete(cfg: &Config, project_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => AgentlessScanningAPI::with_client_and_config(dd_cfg, c),
        None => AgentlessScanningAPI::with_config(dd_cfg),
    };
    api.delete_gcp_scan_options(project_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete GCP scan options: {e:?}"))?;
    println!("GCP scan options for project '{project_id}' deleted successfully.");
    Ok(())
}
