use anyhow::Result;
use datadog_api_client::datadogV2::api_test_optimization::{
    SearchFlakyTestsOptionalParams, TestOptimizationAPI,
};

use crate::client;
use crate::config::Config;
use crate::formatter;

// ---- Service Settings ----

pub async fn settings_get(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => TestOptimizationAPI::with_client_and_config(dd_cfg, c),
        None => TestOptimizationAPI::with_config(dd_cfg),
    };
    let body = crate::util::read_json_file(file)?;
    let resp = api
        .get_test_optimization_service_settings(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get test optimization service settings: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn settings_update(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => TestOptimizationAPI::with_client_and_config(dd_cfg, c),
        None => TestOptimizationAPI::with_config(dd_cfg),
    };
    let body = crate::util::read_json_file(file)?;
    let resp = api
        .update_test_optimization_service_settings(body)
        .await
        .map_err(|e| {
            anyhow::anyhow!("failed to update test optimization service settings: {e:?}")
        })?;
    formatter::output(cfg, &resp)
}

pub async fn settings_delete(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => TestOptimizationAPI::with_client_and_config(dd_cfg, c),
        None => TestOptimizationAPI::with_config(dd_cfg),
    };
    let body = crate::util::read_json_file(file)?;
    api.delete_test_optimization_service_settings(body)
        .await
        .map_err(|e| {
            anyhow::anyhow!("failed to delete test optimization service settings: {e:?}")
        })?;
    eprintln!("Service settings deleted successfully.");
    Ok(())
}

// ---- Flaky Tests ----

pub async fn flaky_tests_search(cfg: &Config, file: Option<String>) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => TestOptimizationAPI::with_client_and_config(dd_cfg, c),
        None => TestOptimizationAPI::with_config(dd_cfg),
    };
    let params = if let Some(f) = file {
        let body = crate::util::read_json_file(&f)?;
        SearchFlakyTestsOptionalParams::default().body(body)
    } else {
        use datadog_api_client::datadogV2::model::{
            FlakyTestsSearchPageOptions, FlakyTestsSearchRequest,
            FlakyTestsSearchRequestAttributes, FlakyTestsSearchRequestData,
            FlakyTestsSearchRequestDataType,
        };
        let page_opts = FlakyTestsSearchPageOptions::new().limit(100);
        let attrs = FlakyTestsSearchRequestAttributes::new().page(page_opts);
        let data = FlakyTestsSearchRequestData::new()
            .attributes(attrs)
            .type_(FlakyTestsSearchRequestDataType::SEARCH_FLAKY_TESTS_REQUEST);
        let body = FlakyTestsSearchRequest::new().data(data);
        SearchFlakyTestsOptionalParams::default().body(body)
    };
    let resp = api
        .search_flaky_tests(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to search flaky tests: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn flaky_tests_update(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => TestOptimizationAPI::with_client_and_config(dd_cfg, c),
        None => TestOptimizationAPI::with_config(dd_cfg),
    };
    let body = crate::util::read_json_file(file)?;
    let resp = api
        .update_flaky_tests(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update flaky tests: {e:?}"))?;
    formatter::output(cfg, &resp)
}
