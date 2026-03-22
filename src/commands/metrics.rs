use anyhow::Result;

/// Matches a metric name against a glob pattern where `*` is a wildcard.
/// Falls back to substring match when no `*` is present.
fn matches_glob(text: &str, pattern: &str) -> bool {
    if !pattern.contains('*') {
        return text.contains(pattern);
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    let mut remaining = text;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !remaining.starts_with(part) {
                return false;
            }
            remaining = &remaining[part.len()..];
        } else if i == parts.len() - 1 {
            return remaining.ends_with(part);
        } else {
            match remaining.find(part) {
                Some(pos) => remaining = &remaining[pos + part.len()..],
                None => return false,
            }
        }
    }
    true
}

#[cfg(not(target_arch = "wasm32"))]
use datadog_api_client::datadogV1::api_metrics::{
    ListActiveMetricsOptionalParams, MetricsAPI as MetricsV1API,
};
#[cfg(not(target_arch = "wasm32"))]
use datadog_api_client::datadogV1::model::MetricMetadata;
#[cfg(not(target_arch = "wasm32"))]
use datadog_api_client::datadogV2::api_metrics::MetricsAPI as MetricsV2API;
#[cfg(not(target_arch = "wasm32"))]
use datadog_api_client::datadogV2::model::MetricPayload;

#[cfg(not(target_arch = "wasm32"))]
use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

#[cfg(not(target_arch = "wasm32"))]
pub async fn list(cfg: &Config, filter: Option<String>, from: String) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => MetricsV1API::with_client_and_config(dd_cfg, c),
        None => MetricsV1API::with_config(dd_cfg),
    };

    let from_ts = util::parse_time_to_unix(&from)?;
    let params = ListActiveMetricsOptionalParams::default();

    let resp = api
        .list_active_metrics(from_ts, params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list metrics: {e:?}"))?;

    // Client-side filter if provided
    if let Some(pattern) = filter {
        let pattern = pattern.to_lowercase();
        let metrics = resp.metrics.as_deref().unwrap_or(&[]);
        let filtered: Vec<&str> = metrics
            .iter()
            .filter(|m| matches_glob(&m.to_lowercase(), &pattern))
            .map(|m| m.as_str())
            .collect();
        let output = serde_json::json!({
            "from": resp.from,
            "metrics": filtered,
        });
        return formatter::output(cfg, &output);
    }

    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn list(cfg: &Config, filter: Option<String>, from: String) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let query_params = vec![("from", from_ts.to_string())];
    let data = crate::api::get(cfg, "/api/v2/metrics", &query_params).await?;

    // Client-side filter if provided
    if let Some(pattern) = filter {
        let pattern = pattern.to_lowercase();
        let metrics = data
            .get("metrics")
            .and_then(|v| v.as_array())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let filtered: Vec<&str> = metrics
            .iter()
            .filter_map(|m| m.as_str())
            .filter(|s| matches_glob(&s.to_lowercase(), &pattern))
            .collect();
        let output = serde_json::json!({
            "from": data.get("from"),
            "metrics": filtered,
        });
        return crate::formatter::output(cfg, &output);
    }

    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn search(cfg: &Config, query: String, from: String, to: String) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => MetricsV1API::with_client_and_config(dd_cfg, c),
        None => MetricsV1API::with_config(dd_cfg),
    };

    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;

    let resp = api
        .query_metrics(from_ts, to_ts, query)
        .await
        .map_err(|e| anyhow::anyhow!("failed to query metrics: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn search(cfg: &Config, query: String, from: String, to: String) -> Result<()> {
    let query_params = vec![("q", format!("metrics:{query}"))];
    let data = crate::api::get(cfg, "/api/v1/search", &query_params).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn metadata_get(cfg: &Config, metric_name: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => MetricsV1API::with_client_and_config(dd_cfg, c),
        None => MetricsV1API::with_config(dd_cfg),
    };
    let resp = api
        .get_metric_metadata(metric_name.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get metric metadata: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn metadata_get(cfg: &Config, metric_name: &str) -> Result<()> {
    let path = format!("/api/v1/metrics/{metric_name}");
    let data = crate::api::get(cfg, &path, &[]).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn query(cfg: &Config, query: String, from: String, to: String) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => MetricsV1API::with_client_and_config(dd_cfg, c),
        None => MetricsV1API::with_config(dd_cfg),
    };

    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;

    let resp = api
        .query_metrics(from_ts, to_ts, query)
        .await
        .map_err(|e| anyhow::anyhow!("failed to query metrics: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn query(cfg: &Config, query: String, from: String, to: String) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;
    let body = serde_json::json!({
        "formulas": [{ "formula": query }],
        "from": from_ts * 1000,
        "to": to_ts * 1000
    });
    let data = crate::api::post(cfg, "/api/v2/query/timeseries", &body).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn metadata_update(cfg: &Config, metric_name: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => MetricsV1API::with_client_and_config(dd_cfg, c),
        None => MetricsV1API::with_config(dd_cfg),
    };
    let body: MetricMetadata = util::read_json_file(file)?;
    let resp = api
        .update_metric_metadata(metric_name.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update metric metadata: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn metadata_update(cfg: &Config, metric_name: &str, file: &str) -> Result<()> {
    let body: serde_json::Value = util::read_json_file(file)?;
    let path = format!("/api/v1/metrics/{metric_name}");
    let data = crate::api::put(cfg, &path, &body).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn submit(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => MetricsV2API::with_client_and_config(dd_cfg, c),
        None => MetricsV2API::with_config(dd_cfg),
    };
    let body: MetricPayload = util::read_json_file(file)?;
    let resp = api
        .submit_metrics(
            body,
            datadog_api_client::datadogV2::api_metrics::SubmitMetricsOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to submit metrics: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn submit(cfg: &Config, file: &str) -> Result<()> {
    let body: serde_json::Value = util::read_json_file(file)?;
    let data = crate::api::post(cfg, "/api/v2/series", &body).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn tags_list(cfg: &Config, metric_name: &str) -> Result<()> {
    use datadog_api_client::datadogV2::api_metrics::ListTagsByMetricNameOptionalParams;

    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => MetricsV2API::with_client_and_config(dd_cfg, c),
        None => MetricsV2API::with_config(dd_cfg),
    };
    let resp = api
        .list_tags_by_metric_name(
            metric_name.to_string(),
            ListTagsByMetricNameOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to list tags for metric {metric_name}: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn tags_list(cfg: &Config, metric_name: &str) -> Result<()> {
    let path = format!("/api/v2/metrics/{metric_name}/tags");
    let data = crate::api::get(cfg, &path, &[]).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(test)]
mod tests {
    use super::matches_glob;

    #[test]
    fn test_glob_suffix_wildcard() {
        assert!(matches_glob("system.cpu.user", "system.*"));
        assert!(matches_glob("system.mem.used", "system.*"));
        assert!(!matches_glob("datadog.agent.cpu", "system.*"));
    }

    #[test]
    fn test_glob_prefix_wildcard() {
        assert!(matches_glob("avg.system.cpu", "*.cpu"));
        assert!(!matches_glob("system.cpu.user", "*.cpu"));
    }

    #[test]
    fn test_glob_both_wildcards() {
        assert!(matches_glob("system.cpu.user", "*.cpu.*"));
        assert!(matches_glob("datadog.system.cpu.idle", "*.cpu.*"));
        assert!(!matches_glob("system.mem.used", "*.cpu.*"));
    }

    #[test]
    fn test_glob_no_wildcard_substring() {
        assert!(matches_glob("datadog.estimated_usage.platform", "platform"));
        assert!(matches_glob("system.cpu.user", "cpu"));
        assert!(!matches_glob("system.mem.used", "cpu"));
    }

    #[test]
    fn test_glob_wildcard_only() {
        assert!(matches_glob("anything", "*"));
        assert!(matches_glob("", "*"));
    }

    #[test]
    fn test_glob_exact_with_dot() {
        assert!(matches_glob("system.cpu.user", "system.cpu.user"));
        assert!(!matches_glob("system.cpu.idle", "system.cpu.user"));
    }
}
