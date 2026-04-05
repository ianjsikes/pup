use anyhow::{bail, Result};
use datadog_api_client::datadogV2::api_incident_services::{
    GetIncidentServiceOptionalParams, IncidentServicesAPI, ListIncidentServicesOptionalParams,
};
use datadog_api_client::datadogV2::api_incident_teams::{
    GetIncidentTeamOptionalParams, IncidentTeamsAPI, ListIncidentTeamsOptionalParams,
};
use datadog_api_client::datadogV2::api_incidents::{
    CreateGlobalIncidentHandleOptionalParams, GetIncidentOptionalParams,
    ImportIncidentOptionalParams, IncidentsAPI, ListGlobalIncidentHandlesOptionalParams,
    ListIncidentAttachmentsOptionalParams, SearchIncidentsOptionalParams,
    UpdateGlobalIncidentHandleOptionalParams,
};
use datadog_api_client::datadogV2::model::{IncidentImportRequest, IncidentSearchSortOrder};

use crate::client;
use crate::config::Config;
use crate::formatter;
use crate::util;

// ---------------------------------------------------------------------------
// Helper: build an IncidentsAPI with bearer-token support
// ---------------------------------------------------------------------------

fn make_api(cfg: &Config) -> IncidentsAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => IncidentsAPI::with_client_and_config(dd_cfg, c),
        None => IncidentsAPI::with_config(dd_cfg),
    }
}

// ---------------------------------------------------------------------------
// Core incident operations
// ---------------------------------------------------------------------------

pub async fn list(cfg: &Config, limit: i64) -> Result<()> {
    let api = make_api(cfg);
    let params = SearchIncidentsOptionalParams::default()
        .page_size(limit)
        .sort(IncidentSearchSortOrder::CREATED_DESCENDING);
    let resp = api
        .search_incidents("state:active".to_string(), params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list incidents: {:?}", e))?;
    formatter::output(cfg, &resp)?;
    Ok(())
}

pub async fn get(cfg: &Config, incident_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_incident(
            incident_id.to_string(),
            GetIncidentOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to get incident: {:?}", e))?;
    formatter::output(cfg, &resp)
}

// ---------------------------------------------------------------------------
// Attachments
// ---------------------------------------------------------------------------

pub async fn attachments_list(cfg: &Config, incident_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .list_incident_attachments(
            incident_id.to_string(),
            ListIncidentAttachmentsOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to list incident attachments: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn attachments_delete(
    cfg: &Config,
    incident_id: &str,
    attachment_id: &str,
) -> Result<()> {
    let url = format!(
        "{}/api/v2/incidents/{}/attachments/{}",
        cfg.api_base_url(),
        incident_id,
        attachment_id
    );
    let client = reqwest::Client::new();
    let mut req = client.delete(&url);

    if let Some(token) = &cfg.access_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    } else if let (Some(api_key), Some(app_key)) = (&cfg.api_key, &cfg.app_key) {
        req = req
            .header("DD-API-KEY", api_key.as_str())
            .header("DD-APPLICATION-KEY", app_key.as_str());
    } else {
        bail!("no authentication configured");
    }

    let resp = req.header("Accept", "application/json").send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!("failed to delete incident attachment (HTTP {status}): {body}");
    }
    println!("Incident attachment {attachment_id} deleted from incident {incident_id}.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Global incident settings
// ---------------------------------------------------------------------------

pub async fn settings_get(cfg: &Config) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_global_incident_settings()
        .await
        .map_err(|e| anyhow::anyhow!("failed to get incident settings: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn settings_update(cfg: &Config, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_global_incident_settings(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update incident settings: {:?}", e))?;
    formatter::output(cfg, &resp)
}

// ---------------------------------------------------------------------------
// Global incident handles
// ---------------------------------------------------------------------------

pub async fn handles_list(cfg: &Config) -> Result<()> {
    let api = make_api(cfg);
    let params = ListGlobalIncidentHandlesOptionalParams::default();
    let resp = api
        .list_global_incident_handles(params)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list incident handles: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_create(cfg: &Config, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_global_incident_handle(body, CreateGlobalIncidentHandleOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to create incident handle: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_update(cfg: &Config, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_global_incident_handle(body, UpdateGlobalIncidentHandleOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to update incident handle: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn handles_delete(cfg: &Config, _handle_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_global_incident_handle()
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete incident handle: {:?}", e))?;
    println!("Incident handle deleted.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Postmortem templates
// ---------------------------------------------------------------------------

pub async fn postmortem_templates_list(cfg: &Config) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .list_incident_postmortem_templates()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list postmortem templates: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn postmortem_templates_get(cfg: &Config, template_id: &str) -> Result<()> {
    let api = make_api(cfg);
    let resp = api
        .get_incident_postmortem_template(template_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to get postmortem template: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn postmortem_templates_create(cfg: &Config, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .create_incident_postmortem_template(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create postmortem template: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn postmortem_templates_update(
    cfg: &Config,
    template_id: &str,
    file: &str,
) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .update_incident_postmortem_template(template_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update postmortem template: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn postmortem_templates_delete(cfg: &Config, template_id: &str) -> Result<()> {
    let api = make_api(cfg);
    api.delete_incident_postmortem_template(template_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete postmortem template: {:?}", e))?;
    println!("Postmortem template {template_id} deleted.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Incident teams
// ---------------------------------------------------------------------------

fn make_teams_api(cfg: &Config) -> IncidentTeamsAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => IncidentTeamsAPI::with_client_and_config(dd_cfg, c),
        None => IncidentTeamsAPI::with_config(dd_cfg),
    }
}

pub async fn teams_list(cfg: &Config) -> Result<()> {
    let api = make_teams_api(cfg);
    let resp = api
        .list_incident_teams(ListIncidentTeamsOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list incident teams: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn teams_get(cfg: &Config, team_id: &str) -> Result<()> {
    let api = make_teams_api(cfg);
    let resp = api
        .get_incident_team(
            team_id.to_string(),
            GetIncidentTeamOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to get incident team: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn teams_create(cfg: &Config, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_teams_api(cfg);
    let resp = api
        .create_incident_team(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create incident team: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn teams_update(cfg: &Config, team_id: &str, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_teams_api(cfg);
    let resp = api
        .update_incident_team(team_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update incident team: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn teams_delete(cfg: &Config, team_id: &str) -> Result<()> {
    let api = make_teams_api(cfg);
    api.delete_incident_team(team_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete incident team: {:?}", e))?;
    println!("Incident team {team_id} deleted.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Incident services
// ---------------------------------------------------------------------------

fn make_services_api(cfg: &Config) -> IncidentServicesAPI {
    let dd_cfg = client::make_dd_config(cfg);
    match client::make_bearer_client(cfg) {
        Some(c) => IncidentServicesAPI::with_client_and_config(dd_cfg, c),
        None => IncidentServicesAPI::with_config(dd_cfg),
    }
}

pub async fn services_list(cfg: &Config) -> Result<()> {
    let api = make_services_api(cfg);
    let resp = api
        .list_incident_services(ListIncidentServicesOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to list incident services: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn services_get(cfg: &Config, service_id: &str) -> Result<()> {
    let api = make_services_api(cfg);
    let resp = api
        .get_incident_service(
            service_id.to_string(),
            GetIncidentServiceOptionalParams::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("failed to get incident service: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn services_create(cfg: &Config, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_services_api(cfg);
    let resp = api
        .create_incident_service(body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create incident service: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn services_update(cfg: &Config, service_id: &str, file: &str) -> Result<()> {
    let body = util::read_json_file(file)?;
    let api = make_services_api(cfg);
    let resp = api
        .update_incident_service(service_id.to_string(), body)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update incident service: {:?}", e))?;
    formatter::output(cfg, &resp)
}

pub async fn services_delete(cfg: &Config, service_id: &str) -> Result<()> {
    let api = make_services_api(cfg);
    api.delete_incident_service(service_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete incident service: {:?}", e))?;
    println!("Incident service {service_id} deleted.");
    Ok(())
}

// ---- Import ----

pub async fn import(cfg: &Config, file: &str) -> Result<()> {
    let body: IncidentImportRequest = util::read_json_file(file)?;
    let api = make_api(cfg);
    let resp = api
        .import_incident(body, ImportIncidentOptionalParams::default())
        .await
        .map_err(|e| anyhow::anyhow!("failed to import incident: {e:?}"))?;
    formatter::output(cfg, &resp)
}
