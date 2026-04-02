use anyhow::Result;

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

// ---- Projects ----

pub async fn projects_create(cfg: &Config, file: &str) -> Result<()> {
    let body: serde_json::Value = util::read_json_file(file)?;
    let resp = client::raw_post(cfg, "/api/v2/llm-obs/v1/projects", body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create LLM obs project: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn projects_list(cfg: &Config) -> Result<()> {
    let resp = client::raw_get(cfg, "/api/v2/llm-obs/v1/projects", &[])
        .await
        .map_err(|e| anyhow::anyhow!("failed to list LLM obs projects: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- Experiments ----

pub async fn experiments_create(cfg: &Config, file: &str) -> Result<()> {
    let body: serde_json::Value = util::read_json_file(file)?;
    let resp = client::raw_post(cfg, "/api/v2/llm-obs/v1/experiments", body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create LLM obs experiment: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn experiments_list(
    cfg: &Config,
    filter_project_id: Option<String>,
    filter_dataset_id: Option<String>,
) -> Result<()> {
    let pid = filter_project_id.unwrap_or_default();
    let did = filter_dataset_id.unwrap_or_default();
    let mut query: Vec<(&str, &str)> = vec![];
    if !pid.is_empty() {
        query.push(("filter[project_id]", &pid));
    }
    if !did.is_empty() {
        query.push(("filter[dataset_id]", &did));
    }
    let resp = client::raw_get(cfg, "/api/v2/llm-obs/v1/experiments", &query)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list LLM obs experiments: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn experiments_update(cfg: &Config, experiment_id: &str, file: &str) -> Result<()> {
    let body: serde_json::Value = util::read_json_file(file)?;
    let resp = client::raw_patch(
        cfg,
        &format!("/api/v2/llm-obs/v1/experiments/{experiment_id}"),
        body,
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to update LLM obs experiment: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn experiments_delete(cfg: &Config, file: &str) -> Result<()> {
    let body: serde_json::Value = util::read_json_file(file)?;
    client::raw_post(cfg, "/api/v2/llm-obs/v1/experiments/delete", body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete LLM obs experiments: {e:?}"))?;
    eprintln!("LLM obs experiments deleted.");
    Ok(())
}

// ---- Datasets ----

pub async fn datasets_create(cfg: &Config, project_id: &str, file: &str) -> Result<()> {
    let body: serde_json::Value = util::read_json_file(file)?;
    let resp = client::raw_post(
        cfg,
        &format!("/api/v2/llm-obs/v1/{project_id}/datasets"),
        body,
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to create LLM obs dataset: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn datasets_list(cfg: &Config, project_id: &str) -> Result<()> {
    let resp = client::raw_get(
        cfg,
        &format!("/api/v2/llm-obs/v1/{project_id}/datasets"),
        &[],
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to list LLM obs datasets: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- Experiment analytics ----

pub async fn experiments_summary(cfg: &Config, experiment_id: &str) -> Result<()> {
    let body = serde_json::json!({ "experiment_id": experiment_id });
    let resp = client::raw_post(cfg, "/api/unstable/llm-obs-mcp/v1/experiment/summary", body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get experiment summary: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[allow(clippy::too_many_arguments)]
pub async fn experiments_events_list(
    cfg: &Config,
    experiment_id: &str,
    limit: u32,
    offset: u32,
    filter_dimension_key: Option<String>,
    filter_dimension_value: Option<String>,
    filter_metric_label: Option<String>,
    sort_by_metric: Option<String>,
    sort_direction: &str,
) -> Result<()> {
    let mut body = serde_json::json!({
        "experiment_id": experiment_id,
        "limit": limit,
        "offset": offset,
        "sort_direction": sort_direction,
    });
    if let Some(k) = filter_dimension_key {
        body["filter_dimension_key"] = serde_json::json!(k);
    }
    if let Some(v) = filter_dimension_value {
        body["filter_dimension_value"] = serde_json::json!(v);
    }
    if let Some(l) = filter_metric_label {
        body["filter_metric_label"] = serde_json::json!(l);
    }
    if let Some(m) = sort_by_metric {
        body["sort_by_metric_label"] = serde_json::json!(m);
    }
    let resp = client::raw_post(cfg, "/api/unstable/llm-obs-mcp/v1/experiment/events", body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list experiment events: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn experiments_events_get(
    cfg: &Config,
    experiment_id: &str,
    event_id: &str,
) -> Result<()> {
    let body = serde_json::json!({ "experiment_id": experiment_id, "event_id": event_id });
    let resp = client::raw_post(cfg, "/api/unstable/llm-obs-mcp/v1/experiment/event", body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get experiment event: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn experiments_metric_values(
    cfg: &Config,
    experiment_id: &str,
    metric_label: &str,
    segment_by_dimension: Option<String>,
    segment_dimension_value: Option<String>,
) -> Result<()> {
    let mut body =
        serde_json::json!({ "experiment_id": experiment_id, "metric_label": metric_label });
    if let Some(d) = segment_by_dimension {
        body["segment_by_dimension"] = serde_json::json!(d);
    }
    if let Some(v) = segment_dimension_value {
        body["segment_dimension_value"] = serde_json::json!(v);
    }
    let resp = client::raw_post(
        cfg,
        "/api/unstable/llm-obs-mcp/v1/experiment/metric-values",
        body,
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to get experiment metric values: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn experiments_dimension_values(
    cfg: &Config,
    experiment_id: &str,
    dimension_key: &str,
) -> Result<()> {
    let body =
        serde_json::json!({ "experiment_id": experiment_id, "dimension_key": dimension_key });
    let resp = client::raw_post(
        cfg,
        "/api/unstable/llm-obs-mcp/v1/experiment/dimension-values",
        body,
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to get experiment dimension values: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- Spans ----

#[allow(clippy::too_many_arguments)]
pub async fn spans_search(
    cfg: &Config,
    query: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    span_kind: Option<String>,
    span_name: Option<String>,
    ml_app: Option<String>,
    root_spans_only: bool,
    from: String,
    to: String,
    limit: u32,
    cursor: Option<String>,
) -> Result<()> {
    let mut body = serde_json::json!({ "limit": limit });
    if root_spans_only {
        body["root_spans_only"] = serde_json::json!(true);
    }
    if let Some(q) = query {
        body["query"] = serde_json::json!(q);
    }
    if let Some(t) = trace_id {
        body["trace_id"] = serde_json::json!(t);
    }
    if let Some(s) = span_id {
        body["span_id"] = serde_json::json!(s);
    }
    if let Some(k) = span_kind {
        body["span_kind"] = serde_json::json!(k);
    }
    if let Some(n) = span_name {
        body["span_name"] = serde_json::json!(n);
    }
    if let Some(a) = ml_app {
        body["ml_app"] = serde_json::json!(a);
    }
    let from_ms = crate::util::parse_time_to_unix_millis(&from)
        .map_err(|e| anyhow::anyhow!("invalid --from value: {e}"))?;
    body["from"] = serde_json::json!(from_ms.to_string());

    let to_ms = crate::util::parse_time_to_unix_millis(&to)
        .map_err(|e| anyhow::anyhow!("invalid --to value: {e}"))?;
    body["to"] = serde_json::json!(to_ms.to_string());
    if let Some(c) = cursor {
        body["cursor"] = serde_json::json!(c);
    }
    let resp = client::raw_post(cfg, "/api/unstable/llm-obs-mcp/v1/trace/search-spans", body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to search spans: {e:?}"))?;
    formatter::output(cfg, &resp)
}
