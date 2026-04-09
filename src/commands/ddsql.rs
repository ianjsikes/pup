use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::time::Duration;

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::useragent;
use crate::util;
use crate::version;

fn client_id() -> String {
    let agent = useragent::detect_agent_info();
    if agent.detected {
        format!("pup/{}/{}", version::VERSION, agent.name)
    } else {
        format!("pup/{}", version::VERSION)
    }
}

/// Build a request for the Advanced Query API (tabular/scalar endpoint).
///
/// Endpoint: POST /api/unstable/advanced/query/tabular
/// Supports OAuth tokens and API keys (unlike the UI-only analysis-workspace endpoint).
fn build_advanced_table_request(
    query: &str,
    from: &str,
    to: &str,
    limit: Option<i32>,
) -> Result<Value> {
    let from_ms =
        util::parse_time_to_unix_millis(from).map_err(|e| anyhow!("invalid --from: {e}"))?;
    let to_ms = util::parse_time_to_unix_millis(to).map_err(|e| anyhow!("invalid --to: {e}"))?;

    let mut query_body = json!({
        "dataset": "user_query",
        "time_window": { "from": from_ms, "to": to_ms },
    });
    if let Some(l) = limit {
        query_body["limit"] = json!(l);
    }

    Ok(json!({
        "data": {
            "type": "analysis_workspace_query_request",
            "attributes": {
                "datasets": [{
                    "data_source": "analysis_dataset",
                    "name": "user_query",
                    "query": {
                        "type": "sql_analysis",
                        "sql_query": query,
                    }
                }],
                "query": query_body,
            }
        },
        "meta": {
            "client_id": client_id(),
            "user_query_id": uuid::Uuid::new_v4().to_string(),
            "use_async_querying": true,
        }
    }))
}

/// Extract the query status from an async query response.
///
/// Returns `Ok(Some(query_id))` if the query is still running,
/// `Ok(None)` if the query is done, or an error if the status is unexpected.
fn extract_query_status(resp: &Value) -> Result<Option<String>> {
    // API returns meta.responses[0].queries[0] (new shape) or meta.queries[0] (old shape).
    let query_meta = resp
        .pointer("/meta/responses/0/queries/0")
        .or_else(|| resp.pointer("/meta/queries/0"))
        .ok_or_else(|| anyhow!("unexpected response: missing query status in meta"))?;

    let status = query_meta
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("unexpected response: missing query status"))?;

    match status {
        "done" => Ok(None),
        "running" => {
            let query_id = query_meta
                .get("query_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("unexpected response: running query missing query_id"))?
                .to_string();
            Ok(Some(query_id))
        }
        other => Err(anyhow!("unexpected query status: {other}")),
    }
}

/// Build a polling request for an in-progress async query.
///
/// Endpoint: POST /api/unstable/advanced/query/tabular/fetch
/// Same shape as the originating request, but with an additional `query_id` in attributes.
fn build_fetch_request(base_body: &Value, query_id: &str) -> Value {
    let mut fetch_body = base_body.clone();
    fetch_body["data"]["attributes"]["query_id"] = json!(query_id);
    fetch_body
}

/// Submit an async query and poll until completion.
///
/// Sends the initial request and, if the query is still running, polls the fetch
/// endpoint until the query completes.
async fn execute_async_query(cfg: &Config, body: Value) -> Result<Value> {
    let resp = client::raw_post(cfg, "/api/unstable/advanced/query/tabular", body.clone()).await?;

    let mut query_id = match extract_query_status(&resp)? {
        None => return Ok(resp),
        Some(id) => id,
    };

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let fetch_body = build_fetch_request(&body, &query_id);
        let poll_resp = client::raw_post(
            cfg,
            "/api/unstable/advanced/query/tabular/fetch",
            fetch_body,
        )
        .await?;

        match extract_query_status(&poll_resp)? {
            None => return Ok(poll_resp),
            Some(id) => query_id = id,
        }
    }
}

/// Execute a DDSQL query and return the result as a row-based JSON array.
///
/// Shared function used by both `ddsql table` and `security findings-analyze`.
pub async fn execute_ddsql_query(
    cfg: &Config,
    query: &str,
    from: &str,
    to: &str,
    limit: Option<i32>,
) -> Result<Value> {
    let body = build_advanced_table_request(query, from, to, limit)?;
    let data = execute_async_query(cfg, body).await?;
    columnar_to_rows(&data)
}

pub async fn table(
    cfg: &Config,
    query: &str,
    from: &str,
    to: &str,
    _interval: Option<i64>,
    limit: Option<i32>,
    _offset: Option<i32>,
) -> Result<()> {
    let rows = execute_ddsql_query(cfg, query, from, to, limit).await?;
    formatter::output(cfg, &rows)
}

pub async fn time_series(
    cfg: &Config,
    query: &str,
    from: &str,
    to: &str,
    _interval: Option<i64>,
    limit: i32,
) -> Result<()> {
    let body = build_advanced_table_request(query, from, to, Some(limit))?;
    let data = execute_async_query(cfg, body).await?;
    let rows = columnar_to_rows(&data)?;
    formatter::output(cfg, &rows)
}

/// Transform a DDSQL columnar response into a row-based JSON array.
///
/// The table endpoint returns columns in one of two shapes:
///   Array: {"data": [{"attributes": {"columns": [...]}}]}
///   Object: {"data": {"attributes": {"columns": [...]}}}
///
/// Each column is: {"name": "col1", "values": ["a", "b"]}
///
/// This transforms it to: [{"col1": "a", "col2": 1}, {"col1": "b", "col2": 2}]
fn columnar_to_rows(resp: &Value) -> Result<Value> {
    // Try array shape first (observed in production), then object shape.
    let columns = resp
        .pointer("/data/0/attributes/columns")
        .or_else(|| resp.pointer("/data/attributes/columns"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("unexpected response: missing columns in response"))?;

    if columns.is_empty() {
        return Ok(json!([]));
    }

    let num_rows = columns[0]
        .get("values")
        .and_then(Value::as_array)
        .map(|v| v.len())
        .unwrap_or(0);

    let mut rows = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let mut row = serde_json::Map::new();
        for col in columns {
            let name = col.get("name").and_then(Value::as_str).unwrap_or("unknown");
            let value = col
                .get("values")
                .and_then(Value::as_array)
                .and_then(|vals| vals.get(i))
                .cloned()
                .unwrap_or(Value::Null);
            row.insert(name.to_string(), value);
        }
        rows.push(Value::Object(row));
    }

    Ok(Value::Array(rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_advanced_table_with_limit() {
        let req = build_advanced_table_request("SELECT 1", "1h", "now", Some(10)).unwrap();

        assert_eq!(req["data"]["type"], "analysis_workspace_query_request");
        let attrs = &req["data"]["attributes"];
        assert_eq!(attrs["datasets"][0]["data_source"], "analysis_dataset");
        assert_eq!(attrs["datasets"][0]["name"], "user_query");
        assert_eq!(attrs["datasets"][0]["query"]["type"], "sql_analysis");
        assert_eq!(attrs["datasets"][0]["query"]["sql_query"], "SELECT 1");
        assert_eq!(attrs["query"]["dataset"], "user_query");
        assert_eq!(attrs["query"]["limit"], 10);
        assert!(attrs["query"]["time_window"]["from"].is_i64());
        assert!(attrs["query"]["time_window"]["to"].is_i64());

        let now_ms = chrono::Utc::now().timestamp() * 1000;
        let from = attrs["query"]["time_window"]["from"].as_i64().unwrap();
        let to = attrs["query"]["time_window"]["to"].as_i64().unwrap();
        assert!((to - now_ms).abs() < 2000);
        assert!((from - (now_ms - 3600000)).abs() < 2000);

        assert_eq!(req["meta"]["use_async_querying"], true);
        assert!(req["meta"]["client_id"]
            .as_str()
            .unwrap_or("")
            .starts_with("pup/"));
        assert!(!req["meta"]["user_query_id"]
            .as_str()
            .unwrap_or("")
            .is_empty());
    }

    #[test]
    fn test_build_advanced_table_no_limit() {
        let req = build_advanced_table_request("SELECT 1", "1h", "now", None).unwrap();

        assert_eq!(req["data"]["type"], "analysis_workspace_query_request");
        assert!(
            req["data"]["attributes"]["query"].get("limit").is_none()
                || req["data"]["attributes"]["query"]["limit"].is_null()
        );
    }

    #[test]
    fn test_build_advanced_table_invalid_from() {
        let err = build_advanced_table_request("SELECT 1", "garbage", "now", None).unwrap_err();
        assert!(err.to_string().contains("invalid --from"));
    }

    #[test]
    fn test_columnar_to_rows_array_shape() {
        // Actual production shape: {"data": [{"attributes": {"columns": [...]}}]}
        let resp: Value = serde_json::from_str(
            r#"{"data":[{"attributes":{"columns":[
                {"name":"host","type":"string","values":["h1","h2"]},
                {"name":"cpu","type":"float64","values":[10,20]}
            ]},"id":"ddsql_response","type":"scalar_response"}]}"#,
        )
        .unwrap();
        let rows = columnar_to_rows(&resp).unwrap();
        let arr = rows.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["host"], "h1");
        assert_eq!(arr[0]["cpu"], 10);
        assert_eq!(arr[1]["host"], "h2");
        assert_eq!(arr[1]["cpu"], 20);
    }

    #[test]
    fn test_columnar_to_rows_object_shape() {
        // Fallback shape: {"data": {"attributes": {"columns": [...]}}}
        let resp: Value = serde_json::from_str(
            r#"{"data":{"attributes":{"columns":[
                {"name":"id","values":[42]}
            ]}}}"#,
        )
        .unwrap();
        let rows = columnar_to_rows(&resp).unwrap();
        let arr = rows.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], 42);
    }

    #[test]
    fn test_columnar_to_rows_empty_columns() {
        let resp: Value =
            serde_json::from_str(r#"{"data":[{"attributes":{"columns":[]}}]}"#).unwrap();
        let rows = columnar_to_rows(&resp).unwrap();
        assert_eq!(rows, json!([]));
    }

    #[test]
    fn test_columnar_to_rows_missing_columns() {
        let resp: Value = serde_json::from_str(r#"{"data":[{"attributes":{}}]}"#).unwrap();
        assert!(columnar_to_rows(&resp).is_err());
    }

    #[test]
    fn test_extract_query_status_done() {
        // Old shape (fallback).
        let resp: Value =
            serde_json::from_str(r#"{"meta":{"queries":[{"status":"done","name":"user_query"}]}}"#)
                .unwrap();
        assert!(extract_query_status(&resp).unwrap().is_none());
    }

    #[test]
    fn test_extract_query_status_done_new_shape() {
        // New shape: meta.responses[0].queries[0].
        let resp: Value = serde_json::from_str(
            r#"{"meta":{"responses":[{"queries":[{"status":"done","name":"user_query"}]}]}}"#,
        )
        .unwrap();
        assert!(extract_query_status(&resp).unwrap().is_none());
    }

    #[test]
    fn test_extract_query_status_running() {
        // Old shape (fallback).
        let resp: Value = serde_json::from_str(
            r#"{"meta":{"queries":[{"status":"running","name":"user_query","query_id":"abc-123"}]}}"#,
        )
        .unwrap();
        assert_eq!(
            extract_query_status(&resp).unwrap(),
            Some("abc-123".to_string())
        );
    }

    #[test]
    fn test_extract_query_status_running_new_shape() {
        // New shape: meta.responses[0].queries[0].
        let resp: Value = serde_json::from_str(
            r#"{"meta":{"responses":[{"queries":[{"status":"running","name":"user_query","query_id":"xyz-789"}]}]}}"#,
        )
        .unwrap();
        assert_eq!(
            extract_query_status(&resp).unwrap(),
            Some("xyz-789".to_string())
        );
    }

    #[test]
    fn test_extract_query_status_missing_meta() {
        let resp: Value = serde_json::from_str(r#"{"data":{}}"#).unwrap();
        assert!(extract_query_status(&resp).is_err());
    }

    #[test]
    fn test_extract_query_status_unexpected_status() {
        let resp: Value = serde_json::from_str(
            r#"{"meta":{"queries":[{"status":"failed","name":"user_query"}]}}"#,
        )
        .unwrap();
        let err = extract_query_status(&resp).unwrap_err();
        assert!(err.to_string().contains("unexpected query status"));
    }

    #[test]
    fn test_build_fetch_request_adds_query_id() {
        let base = build_advanced_table_request("SELECT 1", "1h", "now", None).unwrap();
        let fetch = build_fetch_request(&base, "qid-456");
        assert_eq!(fetch["data"]["attributes"]["query_id"], "qid-456");
        // Original fields are preserved.
        assert_eq!(
            fetch["data"]["attributes"]["datasets"][0]["query"]["sql_query"],
            "SELECT 1"
        );
    }

    #[test]
    fn test_columnar_to_rows_null_values() {
        let resp: Value = serde_json::from_str(
            r#"{"data":[{"attributes":{"columns":[
                {"name":"a","values":[1,null]},
                {"name":"b","values":[null,"x"]}
            ]}}]}"#,
        )
        .unwrap();
        let rows = columnar_to_rows(&resp).unwrap();
        let arr = rows.as_array().unwrap();
        assert_eq!(arr[0]["a"], 1);
        assert!(arr[0]["b"].is_null());
        assert!(arr[1]["a"].is_null());
        assert_eq!(arr[1]["b"], "x");
    }
}
