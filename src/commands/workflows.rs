use anyhow::Result;
use std::collections::BTreeMap;

use datadog_api_client::datadogV2::api_workflow_automation::{
    ListWorkflowInstancesOptionalParams, WorkflowAutomationAPI,
};

use crate::client;
use crate::config::Config;
use crate::formatter::{self, Metadata};
use crate::util;

// ---------------------------------------------------------------------------
// Helper: build a WorkflowAutomationAPI (API key auth only)
// ---------------------------------------------------------------------------

fn make_api(cfg: &Config) -> WorkflowAutomationAPI {
    let dd_cfg = client::make_dd_config(cfg);
    WorkflowAutomationAPI::with_config(dd_cfg)
}

// ---------------------------------------------------------------------------
// Workflow CRUD
// ---------------------------------------------------------------------------

pub async fn get(cfg: &Config, workflow_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_workflow(workflow_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get workflow: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn create(cfg: &Config, file: &str) -> Result<()> {
    let body: datadog_api_client::datadogV2::model::CreateWorkflowRequest =
        util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_workflow(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create workflow: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn update(cfg: &Config, workflow_id: &str, file: &str) -> Result<()> {
    let body: datadog_api_client::datadogV2::model::UpdateWorkflowRequest =
        util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_workflow(workflow_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update workflow: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn delete(cfg: &Config, workflow_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_workflow(workflow_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete workflow: {:?}", e))?;
    eprintln!("Workflow {workflow_id} deleted.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Workflow execution (API trigger only — requires DD_API_KEY + DD_APP_KEY)
// ---------------------------------------------------------------------------

pub async fn run(
    cfg: &Config,
    workflow_id: &str,
    payload: Option<String>,
    payload_file: Option<String>,
    wait: bool,
    timeout: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = WorkflowAutomationAPI::with_config(dd_cfg);

    let input_payload: Option<BTreeMap<String, serde_json::Value>> = match (&payload, &payload_file)
    {
        (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!(
                "cannot specify both --payload and --payload-file"
            ))
        }
        (Some(json_str), None) => Some(serde_json::from_str(json_str)?),
        (None, Some(path)) => {
            let contents = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("failed to read payload file {path:?}: {e}"))?;
            Some(serde_json::from_str(&contents)?)
        }
        (None, None) => None,
    };

    let mut request = datadog_api_client::datadogV2::model::WorkflowInstanceCreateRequest::new();
    if let Some(p) = input_payload {
        request = request.meta(
            datadog_api_client::datadogV2::model::WorkflowInstanceCreateMeta::new().payload(p),
        );
    }

    let response = api
        .create_workflow_instance(workflow_id.to_string(), request)
        .await
        .map_err(|e| anyhow::anyhow!("failed to execute workflow: {:?}", e))?;

    if !wait {
        formatter::output(cfg, &response)?;
        return Ok(());
    }

    let instance_id = response
        .data
        .as_ref()
        .and_then(|d| d.id.as_ref())
        .ok_or_else(|| anyhow::anyhow!("no instance ID in response"))?
        .clone();

    eprintln!("Instance {instance_id} started, waiting for completion...");

    let timeout_duration =
        std::time::Duration::from_millis(crate::util::parse_duration_to_millis(timeout)? as u64);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!(
                "timed out after {} waiting for instance {instance_id}",
                timeout
            ));
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let dd_cfg = client::make_dd_config(cfg);
        let api = WorkflowAutomationAPI::with_config(dd_cfg);
        let status = api
            .get_workflow_instance(workflow_id.to_string(), instance_id.clone())
            .await
            .map_err(|e| anyhow::anyhow!("failed to get instance status: {:?}", e))?;

        let state = status
            .data
            .as_ref()
            .and_then(|d| d.attributes.as_ref())
            .and_then(|a| a.additional_properties.get("instanceStatus"))
            .and_then(|v| v.get("detailsKind"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        eprintln!("  status: {state}");

        // Treat any state other than the known in-progress states as terminal.
        // This avoids polling forever if the API introduces new terminal states.
        match state {
            "" | "IN_PROGRESS" => continue,
            _ => {
                formatter::output(cfg, &status)?;
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Workflow instances
// ---------------------------------------------------------------------------

pub async fn instance_list(cfg: &Config, workflow_id: &str, limit: i64, page: i64) -> Result<()> {
    let api = make_api(cfg);

    let mut params = ListWorkflowInstancesOptionalParams::default();
    params = params.page_size(limit.clamp(1, 100));
    if page > 0 {
        params = params.page_number(page);
    }

    let resp = api
        .list_workflow_instances(workflow_id.to_string(), params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list workflow instances: {:?}", e))?;

    let count = resp.data.as_ref().map(|d| d.len());
    let meta = Metadata {
        count,
        truncated: false,
        command: Some("workflows instances list".to_string()),
        next_action: None,
    };
    formatter::format_and_print(&resp, &cfg.output_format, cfg.agent_mode, Some(&meta))
}

pub async fn instance_get(cfg: &Config, workflow_id: &str, instance_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_workflow_instance(workflow_id.to_string(), instance_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get workflow instance: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn instance_cancel(cfg: &Config, workflow_id: &str, instance_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .cancel_workflow_instance(workflow_id.to_string(), instance_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to cancel workflow instance: {:?}", e))?;
    formatter::output(cfg, &resp)
}
