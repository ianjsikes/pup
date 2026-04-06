use anyhow::Result;
use datadog_api_client::datadogV2::api_datasets::DatasetsAPI;

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn make_api(cfg: &Config) -> DatasetsAPI {
    let dd_cfg = client::make_dd_config(cfg);
    DatasetsAPI::with_config(dd_cfg)
}

pub async fn list(cfg: &Config) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_all_datasets()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list datasets: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn get(cfg: &Config, dataset_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_dataset(dataset_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get dataset: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn create(cfg: &Config, file: &str) -> Result<()> {
    let body: datadog_api_client::datadogV2::model::DatasetCreateRequest =
        util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_dataset(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create dataset: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn update(cfg: &Config, dataset_id: &str, file: &str) -> Result<()> {
    let body: datadog_api_client::datadogV2::model::DatasetUpdateRequest =
        util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_dataset(dataset_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update dataset: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn delete(cfg: &Config, dataset_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_dataset(dataset_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete dataset: {e:?}"))?;
    eprintln!("Dataset {dataset_id} deleted.");
    Ok(())
}
