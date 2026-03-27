use anyhow::Result;
#[cfg(not(target_arch = "wasm32"))]
use datadog_api_client::datadogV2::api_container_images::{
    ContainerImagesAPI, ListContainerImagesOptionalParams,
};
#[cfg(not(target_arch = "wasm32"))]
use datadog_api_client::datadogV2::api_containers::{ContainersAPI, ListContainersOptionalParams};

#[cfg(not(target_arch = "wasm32"))]
use crate::client;
use crate::config::Config;
use crate::formatter;

#[cfg(not(target_arch = "wasm32"))]
pub async fn list(
    cfg: &Config,
    filter_tags: Option<String>,
    group_by: Option<String>,
    sort: Option<String>,
    page_size: Option<i32>,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ContainersAPI::with_client_and_config(dd_cfg, c),
        None => ContainersAPI::with_config(dd_cfg),
    };
    let mut params = ListContainersOptionalParams::default();
    if let Some(v) = filter_tags {
        params = params.filter_tags(v);
    }
    if let Some(v) = group_by {
        params = params.group_by(v);
    }
    if let Some(v) = sort {
        params = params.sort(v);
    }
    if let Some(v) = page_size {
        params = params.page_size(v);
    }
    let resp = api
        .list_containers(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list containers: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn list(
    cfg: &Config,
    filter_tags: Option<String>,
    group_by: Option<String>,
    sort: Option<String>,
    page_size: Option<i32>,
) -> Result<()> {
    let mut query: Vec<(&str, String)> = Vec::new();
    if let Some(v) = filter_tags {
        query.push(("filter[tags]", v));
    }
    if let Some(v) = group_by {
        query.push(("group_by", v));
    }
    if let Some(v) = sort {
        query.push(("sort", v));
    }
    if let Some(v) = page_size {
        query.push(("page[size]", v.to_string()));
    }
    let pairs: Vec<(&str, &str)> = query.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let data = crate::api::get(cfg, "/api/v2/containers", &pairs).await?;
    crate::formatter::output(cfg, &data)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn images_list(
    cfg: &Config,
    filter_tags: Option<String>,
    group_by: Option<String>,
    sort: Option<String>,
    page_size: Option<i32>,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ContainerImagesAPI::with_client_and_config(dd_cfg, c),
        None => ContainerImagesAPI::with_config(dd_cfg),
    };
    let mut params = ListContainerImagesOptionalParams::default();
    if let Some(v) = filter_tags {
        params = params.filter_tags(v);
    }
    if let Some(v) = group_by {
        params = params.group_by(v);
    }
    if let Some(v) = sort {
        params = params.sort(v);
    }
    if let Some(v) = page_size {
        params = params.page_size(v);
    }
    let resp = api
        .list_container_images(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list container images: {e:?}"))?;
    formatter::output(cfg, &resp)
}

#[cfg(target_arch = "wasm32")]
pub async fn images_list(
    cfg: &Config,
    filter_tags: Option<String>,
    group_by: Option<String>,
    sort: Option<String>,
    page_size: Option<i32>,
) -> Result<()> {
    let mut query: Vec<(&str, String)> = Vec::new();
    if let Some(v) = filter_tags {
        query.push(("filter[tags]", v));
    }
    if let Some(v) = group_by {
        query.push(("group_by", v));
    }
    if let Some(v) = sort {
        query.push(("sort", v));
    }
    if let Some(v) = page_size {
        query.push(("page[size]", v.to_string()));
    }
    let pairs: Vec<(&str, &str)> = query.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let data = crate::api::get(cfg, "/api/v2/container_images", &pairs).await?;
    crate::formatter::output(cfg, &data)
}
