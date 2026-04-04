use anyhow::{bail, Result};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn parse_sort(sort: &str) -> Result<&'static str> {
    match sort {
        "asc" | "timestamp" => Ok("asc"),
        "desc" | "-timestamp" => Ok("desc"),
        _ => bail!("invalid --sort value: {sort:?}\nExpected: asc, desc, timestamp, or -timestamp"),
    }
}

fn build_search_body(
    query: String,
    from_ms: i64,
    to_ms: i64,
    limit: i32,
    sort: &str,
) -> Result<serde_json::Value> {
    if limit <= 0 {
        bail!("--limit must be a positive integer");
    }

    Ok(serde_json::json!({
        "list": {
            "indexes": ["databasequery"],
            "limit": limit,
            "search": { "query": query },
            "sorts": [
                { "time": { "order": parse_sort(sort)? } }
            ],
            "time": {
                "from": from_ms,
                "to": to_ms
            }
        }
    }))
}

pub async fn samples_search(
    cfg: &Config,
    query: String,
    from: String,
    to: String,
    limit: i32,
    sort: String,
) -> Result<()> {
    cfg.validate_api_and_app_keys()?;

    let from_ms = util::parse_time_to_unix_millis(&from)?;
    let to_ms = util::parse_time_to_unix_millis(&to)?;
    let body = build_search_body(query, from_ms, to_ms, limit, &sort)?;

    let resp = client::raw_post(cfg, "/api/v1/logs-analytics/list?type=databasequery", body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to search DBM query samples: {e:?}"))?;

    formatter::output(cfg, &resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sort_ascending_values() {
        assert_eq!(parse_sort("asc").unwrap(), "asc");
        assert_eq!(parse_sort("timestamp").unwrap(), "asc");
    }

    #[test]
    fn test_parse_sort_descending_values() {
        assert_eq!(parse_sort("desc").unwrap(), "desc");
        assert_eq!(parse_sort("-timestamp").unwrap(), "desc");
    }

    #[test]
    fn test_parse_sort_invalid() {
        assert!(parse_sort("invalid").is_err());
    }

    #[test]
    fn test_build_search_body() {
        let body = build_search_body(
            "service:db".into(),
            1_700_000_000_000,
            1_700_000_060_000,
            10,
            "desc",
        )
        .unwrap();

        assert_eq!(
            body["list"]["indexes"],
            serde_json::json!(["databasequery"])
        );
        assert_eq!(body["list"]["limit"], 10);
        assert_eq!(body["list"]["search"]["query"], "service:db");
        assert_eq!(
            body["list"]["sorts"],
            serde_json::json!([{ "time": { "order": "desc" } }])
        );
        assert_eq!(body["list"]["time"]["from"], 1_700_000_000_000_i64);
        assert_eq!(body["list"]["time"]["to"], 1_700_000_060_000_i64);
    }

    #[test]
    fn test_build_search_body_rejects_zero_limit() {
        let err = build_search_body(
            "service:db".into(),
            1_700_000_000_000,
            1_700_000_060_000,
            0,
            "desc",
        )
        .unwrap_err();

        assert_eq!(err.to_string(), "--limit must be a positive integer");
    }

    #[test]
    fn test_build_search_body_rejects_negative_limit() {
        let err = build_search_body(
            "service:db".into(),
            1_700_000_000_000,
            1_700_000_060_000,
            -1,
            "desc",
        )
        .unwrap_err();

        assert_eq!(err.to_string(), "--limit must be a positive integer");
    }
}
