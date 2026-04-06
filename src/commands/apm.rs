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

pub async fn service_config_get(
    cfg: &Config,
    service_name: String,
    env: Option<String>,
    service_instance_ids: Option<String>,
) -> Result<()> {
    let mut query = vec![("service_name", service_name.as_str())];
    let env_owned;
    if let Some(e) = &env {
        env_owned = e.clone();
        query.push(("env", env_owned.as_str()));
    }
    let ids_owned;
    if let Some(ids) = &service_instance_ids {
        ids_owned = ids.clone();
        query.push(("service_instance_ids", ids_owned.as_str()));
    }
    let data = client::raw_get(cfg, "/api/unstable/apm/service-config", &query).await?;
    formatter::output(cfg, &data)
}

pub async fn service_library_config_get(
    cfg: &Config,
    service_name: String,
    env: Option<String>,
    language: Option<String>,
    mixed: bool,
) -> Result<()> {
    let mut query = vec![("service_name", service_name.as_str())];
    let env_owned;
    if let Some(e) = &env {
        env_owned = e.clone();
        query.push(("env", env_owned.as_str()));
    }
    let lang_owned;
    if let Some(l) = &language {
        lang_owned = l.clone();
        query.push(("language_name", lang_owned.as_str()));
    }
    if mixed {
        query.push(("is_mixed", "true"));
    }
    let data = client::raw_get(cfg, "/api/unstable/apm/service-library-config", &query).await?;
    formatter::output(cfg, &data)
}
