use anyhow::{bail, Result};
use datadog_api_client::datadogV2::api_rum::{ListRUMEventsOptionalParams, RUMAPI};
use datadog_api_client::datadogV2::api_rum_metrics::RumMetricsAPI;
use datadog_api_client::datadogV2::api_rum_replay_heatmaps::{
    ListReplayHeatmapSnapshotsOptionalParams, RumReplayHeatmapsAPI,
};
use datadog_api_client::datadogV2::api_rum_replay_playlists::{
    ListRumReplayPlaylistsOptionalParams, RumReplayPlaylistsAPI,
};
use datadog_api_client::datadogV2::api_rum_retention_filters::RumRetentionFiltersAPI;
use datadog_api_client::datadogV2::model::{
    RUMApplicationCreate, RUMApplicationCreateAttributes, RUMApplicationCreateRequest,
    RUMApplicationCreateType, RUMApplicationUpdateRequest, RUMQueryFilter, RUMSearchEventsRequest,
    RUMSort, RumMetricCreateRequest, RumMetricUpdateRequest, RumRetentionFilterCreateRequest,
    RumRetentionFilterUpdateRequest,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn apps_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RUMAPI::with_client_and_config(dd_cfg, c),
        None => RUMAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_rum_applications()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list RUM apps: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn apps_get(cfg: &Config, app_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RUMAPI::with_client_and_config(dd_cfg, c),
        None => RUMAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_rum_application(app_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get RUM app: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn apps_create(cfg: &Config, name: &str, app_type: Option<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RUMAPI::with_client_and_config(dd_cfg, c),
        None => RUMAPI::with_config(dd_cfg),
    };
    let mut attrs = RUMApplicationCreateAttributes::new(name.to_string());
    if let Some(t) = app_type {
        attrs = attrs.type_(t);
    }
    let data = RUMApplicationCreate::new(attrs, RUMApplicationCreateType::RUM_APPLICATION_CREATE);
    let body = RUMApplicationCreateRequest::new(data);
    let resp = api
        .create_rum_application(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create RUM app: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn apps_delete(cfg: &Config, app_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RUMAPI::with_client_and_config(dd_cfg, c),
        None => RUMAPI::with_config(dd_cfg),
    };
    api.delete_rum_application(app_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete RUM app: {e:?}"))?;
    println!("Successfully deleted RUM application {app_id}");
    Ok(())
}

pub async fn events_list(
    cfg: &Config,
    query: Option<String>,
    from: String,
    to: String,
    limit: i32,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RUMAPI::with_client_and_config(dd_cfg, c),
        None => RUMAPI::with_config(dd_cfg),
    };

    let from_dt =
        chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&from)?).unwrap();
    let to_dt =
        chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&to)?).unwrap();

    let mut params = ListRUMEventsOptionalParams::default()
        .filter_from(from_dt)
        .filter_to(to_dt)
        .page_limit(limit);
    if let Some(q) = query {
        params = params.filter_query(q);
    }

    let resp = api
        .list_rum_events(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list RUM events: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn sessions_search(
    cfg: &Config,
    query: Option<String>,
    from: String,
    to: String,
    _limit: i32,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RUMAPI::with_client_and_config(dd_cfg, c),
        None => RUMAPI::with_config(dd_cfg),
    };

    let from_str = chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&from)?)
        .unwrap()
        .to_rfc3339();
    let to_str = chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&to)?)
        .unwrap()
        .to_rfc3339();

    let mut filter = RUMQueryFilter::new().from(from_str).to(to_str);
    if let Some(q) = query {
        filter = filter.query(q);
    }

    let body = RUMSearchEventsRequest::new()
        .filter(filter)
        .sort(RUMSort::TIMESTAMP_DESCENDING);

    let resp = api
        .search_rum_events(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to search RUM sessions: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn apps_update(cfg: &Config, app_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RUMAPI::with_client_and_config(dd_cfg, c),
        None => RUMAPI::with_config(dd_cfg),
    };
    let body: RUMApplicationUpdateRequest = crate::util::read_json_file(file)?;
    let resp = api
        .update_rum_application(app_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update RUM app: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- RUM Metrics ----

pub async fn metrics_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumMetricsAPI::with_client_and_config(dd_cfg, c),
        None => RumMetricsAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_rum_metrics()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list RUM metrics: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn metrics_get(cfg: &Config, metric_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumMetricsAPI::with_client_and_config(dd_cfg, c),
        None => RumMetricsAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_rum_metric(metric_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get RUM metric: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn metrics_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumMetricsAPI::with_client_and_config(dd_cfg, c),
        None => RumMetricsAPI::with_config(dd_cfg),
    };
    let body: RumMetricCreateRequest = crate::util::read_json_file(file)?;
    let resp = api
        .create_rum_metric(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create RUM metric: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn metrics_update(cfg: &Config, metric_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumMetricsAPI::with_client_and_config(dd_cfg, c),
        None => RumMetricsAPI::with_config(dd_cfg),
    };
    let body: RumMetricUpdateRequest = crate::util::read_json_file(file)?;
    let resp = api
        .update_rum_metric(metric_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update RUM metric: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn metrics_delete(cfg: &Config, metric_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumMetricsAPI::with_client_and_config(dd_cfg, c),
        None => RumMetricsAPI::with_config(dd_cfg),
    };
    api.delete_rum_metric(metric_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete RUM metric: {e:?}"))?;
    println!("RUM metric {metric_id} deleted.");
    Ok(())
}

// ---- RUM Retention Filters ----

pub async fn retention_filters_list(cfg: &Config, app_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumRetentionFiltersAPI::with_client_and_config(dd_cfg, c),
        None => RumRetentionFiltersAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_retention_filters(app_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list RUM retention filters: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn retention_filters_get(cfg: &Config, app_id: &str, filter_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumRetentionFiltersAPI::with_client_and_config(dd_cfg, c),
        None => RumRetentionFiltersAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_retention_filter(app_id.to_string(), filter_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get RUM retention filter: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn retention_filters_create(cfg: &Config, app_id: &str, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumRetentionFiltersAPI::with_client_and_config(dd_cfg, c),
        None => RumRetentionFiltersAPI::with_config(dd_cfg),
    };
    let body: RumRetentionFilterCreateRequest = crate::util::read_json_file(file)?;
    let resp = api
        .create_retention_filter(app_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create RUM retention filter: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn retention_filters_update(
    cfg: &Config,
    app_id: &str,
    filter_id: &str,
    file: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumRetentionFiltersAPI::with_client_and_config(dd_cfg, c),
        None => RumRetentionFiltersAPI::with_config(dd_cfg),
    };
    let body: RumRetentionFilterUpdateRequest = crate::util::read_json_file(file)?;
    let resp = api
        .update_retention_filter(app_id.to_string(), filter_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update RUM retention filter: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn retention_filters_delete(cfg: &Config, app_id: &str, filter_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumRetentionFiltersAPI::with_client_and_config(dd_cfg, c),
        None => RumRetentionFiltersAPI::with_config(dd_cfg),
    };
    api.delete_retention_filter(app_id.to_string(), filter_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete RUM retention filter: {e:?}"))?;
    println!("RUM retention filter {filter_id} deleted.");
    Ok(())
}

// ---- RUM Sessions ----

pub async fn sessions_list(cfg: &Config, from: String, to: String, limit: i32) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RUMAPI::with_client_and_config(dd_cfg, c),
        None => RUMAPI::with_config(dd_cfg),
    };

    let from_str = chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&from)?)
        .unwrap()
        .to_rfc3339();
    let to_str = chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&to)?)
        .unwrap()
        .to_rfc3339();

    let filter = RUMQueryFilter::new()
        .from(from_str)
        .to(to_str)
        .query("@type:session".to_string());

    let body = RUMSearchEventsRequest::new()
        .filter(filter)
        .sort(RUMSort::TIMESTAMP_DESCENDING)
        .page(datadog_api_client::datadogV2::model::RUMQueryPageOptions::new().limit(limit));

    let resp = api
        .search_rum_events(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list RUM sessions: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- RUM Playlists ----

pub async fn playlists_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumReplayPlaylistsAPI::with_client_and_config(dd_cfg, c),
        None => RumReplayPlaylistsAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_rum_replay_playlists(ListRumReplayPlaylistsOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list RUM playlists: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn playlists_get(cfg: &Config, playlist_id: i32) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumReplayPlaylistsAPI::with_client_and_config(dd_cfg, c),
        None => RumReplayPlaylistsAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_rum_replay_playlist(playlist_id)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get RUM playlist: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- RUM Heatmaps ----

pub async fn heatmaps_query(cfg: &Config, view_name: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RumReplayHeatmapsAPI::with_client_and_config(dd_cfg, c),
        None => RumReplayHeatmapsAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_replay_heatmap_snapshots(
            view_name.to_string(),
            ListReplayHeatmapSnapshotsOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to query RUM heatmaps: {e:?}"))?;
    formatter::output(cfg, &resp)
}

// ---- RUM Aggregate ----

pub struct RumAggregateArgs {
    pub query: String,
    pub from: String,
    pub to: String,
    pub compute: Vec<String>,
    pub group_by: Vec<String>,
    pub limit: i32,
}

fn parse_rum_compute(input: &str) -> Result<(String, Option<String>)> {
    let input = input.trim();
    if input.is_empty() {
        bail!("--compute is required");
    }
    if input == "count" {
        return Ok(("count".into(), None));
    }
    if let Some(paren) = input.find('(') {
        let func = &input[..paren];
        let rest = input[paren + 1..].trim_end_matches(')').trim();
        if func == "percentile" {
            let parts: Vec<&str> = rest.splitn(2, ',').collect();
            if parts.len() != 2 {
                bail!("percentile requires field and value: percentile(@duration, 99)");
            }
            let metric = parts[0].trim().to_string();
            let pct: u32 = parts[1]
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid percentile value: {}", parts[1].trim()))?;
            let agg_name = match pct {
                75 => "pc75",
                90 => "pc90",
                95 => "pc95",
                98 => "pc98",
                99 => "pc99",
                _ => bail!("unsupported percentile: {pct} (supported: 75, 90, 95, 98, 99)"),
            };
            return Ok((agg_name.into(), Some(metric)));
        }
        let metric = rest.to_string();
        let agg_name = match func {
            "avg" | "sum" | "min" | "max" | "median" | "cardinality" => func.to_string(),
            "count" => bail!("count does not accept a field argument; use just 'count'"),
            _ => bail!("unknown aggregation function: {func}"),
        };
        return Ok((agg_name, Some(metric)));
    }
    bail!(
        "invalid --compute format: {input:?}\n\
         Expected: count, avg(@duration), sum(@duration), percentile(@duration, 99), etc."
    )
}

pub async fn aggregate(cfg: &Config, args: RumAggregateArgs) -> Result<()> {
    let RumAggregateArgs {
        query,
        from,
        to,
        mut compute,
        group_by,
        limit,
    } = args;
    if compute.is_empty() {
        compute.push("count".into());
    }
    let from_ms = util::parse_time_to_unix_millis(&from)?;
    let to_ms = util::parse_time_to_unix_millis(&to)?;

    let compute_arr: Vec<serde_json::Value> = compute
        .iter()
        .map(|c| {
            let (aggregation, metric) = parse_rum_compute(c)?;
            let mut obj = serde_json::json!({ "type": "total", "aggregation": aggregation });
            if let Some(m) = metric {
                obj["metric"] = serde_json::Value::String(m);
            }
            Ok(obj)
        })
        .collect::<Result<Vec<_>>>()?;

    let filter = serde_json::json!({
        "query": query,
        "from": from_ms.to_string(),
        "to": to_ms.to_string()
    });

    let mut body = serde_json::json!({
        "filter": filter,
        "compute": compute_arr
    });

    if !group_by.is_empty() {
        let group_by_arr: Vec<serde_json::Value> = group_by
            .iter()
            .map(|facet| {
                let mut obj = serde_json::json!({ "facet": facet });
                if limit > 0 {
                    obj["limit"] = serde_json::json!(limit);
                }
                obj
            })
            .collect();
        body["group_by"] = serde_json::json!(group_by_arr);
    }

    let data = client::raw_post(cfg, "/api/v2/rum/analytics/aggregate", body).await?;
    formatter::output(cfg, &data)?;
    Ok(())
}

/// Split a comma-separated compute string, respecting parentheses.
pub fn split_rum_compute_args(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0u32;
    for ch in input.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    result.push(trimmed);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        result.push(trimmed);
    }
    result
}
