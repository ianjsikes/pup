use anyhow::Result;
use datadog_api_client::datadogV2::api_roles::{ListRolesOptionalParams, RolesAPI};
use datadog_api_client::datadogV2::api_service_accounts::{
    ListServiceAccountApplicationKeysOptionalParams, ServiceAccountsAPI,
};
use datadog_api_client::datadogV2::api_users::{ListUsersOptionalParams, UsersAPI};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

pub async fn list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => UsersAPI::with_client_and_config(dd_cfg, c),
        None => UsersAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_users(ListUsersOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list users: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn get(cfg: &Config, id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => UsersAPI::with_client_and_config(dd_cfg, c),
        None => UsersAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_user(id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get user: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn roles_list(cfg: &Config) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => RolesAPI::with_client_and_config(dd_cfg, c),
        None => RolesAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_roles(ListRolesOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list roles: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn service_accounts_create(cfg: &Config, file: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceAccountsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceAccountsAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_service_account(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create service account: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn service_account_app_keys_create(
    cfg: &Config,
    service_account_id: &str,
    file: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceAccountsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceAccountsAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .create_service_account_application_key(service_account_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create service account application key: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn service_account_app_keys_list(cfg: &Config, service_account_id: &str) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceAccountsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceAccountsAPI::with_config(dd_cfg),
    };
    let resp = api
        .list_service_account_application_keys(
            service_account_id.to_string(),
            ListServiceAccountApplicationKeysOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to list service account application keys: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn service_account_app_keys_get(
    cfg: &Config,
    service_account_id: &str,
    app_key_id: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceAccountsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceAccountsAPI::with_config(dd_cfg),
    };
    let resp = api
        .get_service_account_application_key(service_account_id.to_string(), app_key_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get service account application key: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn service_account_app_keys_update(
    cfg: &Config,
    service_account_id: &str,
    app_key_id: &str,
    file: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceAccountsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceAccountsAPI::with_config(dd_cfg),
    };
    let body = util::read_json_file(file)?;
    let resp = api
        .update_service_account_application_key(
            service_account_id.to_string(),
            app_key_id.to_string(),
            body,
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to update service account application key: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn service_account_app_keys_delete(
    cfg: &Config,
    service_account_id: &str,
    app_key_id: &str,
) -> Result<()> {
    let dd_cfg = client::make_dd_config(cfg);
    let api = match client::make_bearer_client(cfg) {
        Some(c) => ServiceAccountsAPI::with_client_and_config(dd_cfg, c),
        None => ServiceAccountsAPI::with_config(dd_cfg),
    };
    api.delete_service_account_application_key(
        service_account_id.to_string(),
        app_key_id.to_string(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("failed to delete service account application key: {e:?}"))?;
    Ok(())
}
