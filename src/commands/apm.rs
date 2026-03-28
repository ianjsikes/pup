use anyhow::Result;

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn services_list(cfg: &Config, env: String, from: String, to: String) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;
    let path = format!("/api/v2/apm/services?start={from_ts}&end={to_ts}&filter[env]={env}");
    let data = client::raw_get(cfg, &path, &[]).await?;
    formatter::output(cfg, &data)
}

pub async fn services_stats(cfg: &Config, env: String, from: String, to: String) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;
    let path = format!("/api/v2/apm/services/stats?start={from_ts}&end={to_ts}&filter[env]={env}");
    let data = client::raw_get(cfg, &path, &[]).await?;
    formatter::output(cfg, &data)
}

pub async fn entities_list(cfg: &Config, from: String, to: String) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;
    let path = format!("/api/unstable/apm/entities?start={from_ts}&end={to_ts}");
    let data = client::raw_get(cfg, &path, &[]).await?;
    formatter::output(cfg, &data)
}

pub async fn dependencies_list(cfg: &Config, env: String, from: String, to: String) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;
    let path = format!("/api/v1/service_dependencies?start={from_ts}&end={to_ts}&env={env}");
    let data = client::raw_get(cfg, &path, &[]).await?;
    formatter::output(cfg, &data)
}

pub async fn services_operations(
    cfg: &Config,
    service: String,
    env: String,
    from: String,
    to: String,
) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;
    let path =
        format!("/api/v1/trace/operation_names/{service}?env={env}&start={from_ts}&end={to_ts}");
    let data = client::raw_get(cfg, &path, &[]).await?;
    formatter::output(cfg, &data)
}

pub async fn services_resources(
    cfg: &Config,
    service: String,
    operation: String,
    env: String,
    from: String,
    to: String,
) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;
    let path = format!(
        "/api/ui/apm/resources?service={service}&operation={operation}&env={env}&start={from_ts}&end={to_ts}"
    );
    let data = client::raw_get(cfg, &path, &[]).await?;
    formatter::output(cfg, &data)
}

pub async fn flow_map(
    cfg: &Config,
    query: String,
    limit: i64,
    from: String,
    to: String,
) -> Result<()> {
    let from_ts = util::parse_time_to_unix(&from)?;
    let to_ts = util::parse_time_to_unix(&to)?;
    let path =
        format!("/api/ui/apm/flow-map?query={query}&limit={limit}&start={from_ts}&end={to_ts}");
    let data = client::raw_get(cfg, &path, &[]).await?;
    formatter::output(cfg, &data)
}

pub async fn troubleshooting_list(
    cfg: &Config,
    hostname: String,
    timeframe: Option<String>,
) -> Result<()> {
    let path = "/api/unstable/apm/instrumentation-errors";
    let mut query = vec![("hostname", hostname.as_str())];
    let tf_owned;
    if let Some(tf) = &timeframe {
        tf_owned = tf.clone();
        query.push(("timeframe", tf_owned.as_str()));
    }
    let data = client::raw_get(cfg, path, &query).await?;
    formatter::output(cfg, &data)
}
