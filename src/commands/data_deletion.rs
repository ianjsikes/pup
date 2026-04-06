use anyhow::Result;
use datadog_api_client::datadogV2::api_data_deletion::{
    DataDeletionAPI, GetDataDeletionRequestsOptionalParams,
};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn make_api(cfg: &Config) -> DataDeletionAPI {
    let dd_cfg = client::make_dd_config(cfg);
    DataDeletionAPI::with_config(dd_cfg)
}

pub async fn requests_list(
    cfg: &Config,
    product: Option<String>,
    query: Option<String>,
    status: Option<String>,
) -> Result<()> {
    let api = make_api(cfg);
    let mut params = GetDataDeletionRequestsOptionalParams::default();
    if let Some(p) = product {
        params = params.product(p);
    }
    if let Some(q) = query {
        params = params.query(q);
    }
    if let Some(s) = status {
        params = params.status(s);
    }
    let resp = api
        .get_data_deletion_requests(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list data deletion requests: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn requests_create(cfg: &Config, product: &str, file: &str) -> Result<()> {
    let body: datadog_api_client::datadogV2::model::CreateDataDeletionRequestBody =
        util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_data_deletion_request(product.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create data deletion request: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn requests_cancel(cfg: &Config, request_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .cancel_data_deletion_request(request_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to cancel data deletion request: {e:?}"))?;
    formatter::output(cfg, &resp)
}
