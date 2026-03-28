use anyhow::Result;
use datadog_api_client::datadogV2::api_seats::{GetSeatsUsersOptionalParams, SeatsAPI};
use datadog_api_client::datadogV2::model::{AssignSeatsUserRequest, UnassignSeatsUserRequest};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

fn make_api(cfg: &Config) -> SeatsAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => SeatsAPI::with_client_and_config(dd_cfg, c),
        None => SeatsAPI::with_config(dd_cfg),
    }
}

pub async fn users_list(cfg: &Config, product: &str, limit: i32) -> Result<()> {
    let api = make_api(cfg);
    let params = GetSeatsUsersOptionalParams::default().page_limit(limit);
    let resp = api
        .get_seats_users(product.to_string(), params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list seats users: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn users_assign(cfg: &Config, file: &str) -> Result<()> {
    let body: AssignSeatsUserRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .assign_seats_user(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to assign seats: {e:?}"))?;
    formatter::output(cfg, &resp)
}

pub async fn users_unassign(cfg: &Config, file: &str) -> Result<()> {
    let body: UnassignSeatsUserRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    api.unassign_seats_user(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to unassign seats: {e:?}"))?;
    println!("Seats unassigned successfully.");
    Ok(())
}
