use anyhow::Result;
use datadog_api_client::datadogV1::api_usage_metering::{
    GetHourlyUsageAttributionOptionalParams, GetUsageSummaryOptionalParams, UsageMeteringAPI,
};
use datadog_api_client::datadogV1::model::HourlyUsageAttributionUsageType;

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn summary(cfg: &Config, start: String, end: Option<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => UsageMeteringAPI::with_client_and_config(dd_cfg, c),
        None => UsageMeteringAPI::with_config(dd_cfg),
    };

    let start_dt =
        chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&start)?).unwrap();

    let mut params = GetUsageSummaryOptionalParams::default();
    if let Some(e) = end {
        let end_dt =
            chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&e)?).unwrap();
        params = params.end_month(end_dt);
    }

    let resp = api
        .get_usage_summary(start_dt, params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get usage summary: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn hourly(cfg: &Config, start: String, end: Option<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => UsageMeteringAPI::with_client_and_config(dd_cfg, c),
        None => UsageMeteringAPI::with_config(dd_cfg),
    };

    let start_dt =
        chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&start)?).unwrap();

    let mut params = GetHourlyUsageAttributionOptionalParams::default();
    if let Some(e) = end {
        let end_dt =
            chrono::DateTime::from_timestamp_millis(util::parse_time_to_unix_millis(&e)?).unwrap();
        params = params.end_hr(end_dt);
    }

    let resp = api
        .get_hourly_usage_attribution(start_dt, HourlyUsageAttributionUsageType::API_USAGE, params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get hourly usage: {e:?}"))?;
    formatter::output(cfg, &resp)
}
