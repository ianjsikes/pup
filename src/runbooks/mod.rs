use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod engine;
pub mod loader;
pub mod template;

/// A runbook definition loaded from ~/.config/pup/runbooks/<name>.yaml.
#[derive(Deserialize, Serialize, Clone)]
pub struct Runbook {
    pub name: String,
    pub description: Option<String>,
    pub tags: Option<HashMap<String, String>>,
    pub import: Option<Vec<String>>,
    pub vars: Option<HashMap<String, VarDef>>,
    pub steps: Vec<Step>,
}

/// Definition of a runbook variable.
#[derive(Deserialize, Serialize, Clone)]
pub struct VarDef {
    pub description: Option<String>,
    pub required: Option<bool>,
    pub default: Option<String>,
}

/// A reusable step template loaded from ~/.config/pup/runbooks/_templates/<name>.yaml.
/// All fields are optional; a referencing step's fields take precedence.
#[derive(Deserialize, Clone, Default)]
pub struct StepTemplate {
    pub kind: Option<String>,
    pub run: Option<String>,
    pub workflow_id: Option<String>,
    pub inputs: Option<HashMap<String, String>>,
    pub url: Option<String>,
    pub method: Option<String>,
    pub body: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub content_type: Option<String>,
    pub accept: Option<String>,
    pub body_file: Option<String>,
    pub output_file: Option<String>,
    pub message: Option<String>,
    pub on_failure: Option<String>,
    pub when: Option<String>,
    pub optional: Option<bool>,
    pub capture: Option<String>,
    pub poll: Option<PollConfig>,
    pub assert: Option<AssertConfig>,
}

/// A single step in a runbook.
#[derive(Deserialize, Serialize, Clone)]
pub struct Step {
    pub name: String,
    /// "pup" | "shell" | "datadog-workflow" | "confirm" | "http"
    /// May be omitted when a `template` is referenced that supplies the kind.
    #[serde(default)]
    pub kind: String,
    /// Optional template name to inherit fields from (_templates/<name>.yaml).
    /// Step fields override template fields.
    #[serde(default)]
    pub template: Option<String>,
    /// pup or shell command to run
    pub run: Option<String>,
    /// Datadog Workflow ID (for kind: datadog-workflow)
    pub workflow_id: Option<String>,
    /// Inputs for the workflow
    pub inputs: Option<HashMap<String, String>>,
    /// URL for http steps
    pub url: Option<String>,
    /// HTTP method (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS)
    pub method: Option<String>,
    /// Request body template (JSON string, rendered before sending)
    pub body: Option<String>,
    /// Additional HTTP headers (key: value, templates rendered)
    pub headers: Option<HashMap<String, String>>,
    /// Request Content-Type (default: application/json when `body` is set).
    /// Use application/yaml, text/csv, text/plain, or application/octet-stream as needed.
    pub content_type: Option<String>,
    /// Accept header controlling the expected response format (default: application/json).
    pub accept: Option<String>,
    /// Read request body from this file path (template-rendered).
    /// Takes precedence over `body`; use for binary or large payloads.
    pub body_file: Option<String>,
    /// Write response body to this file path (template-rendered).
    /// Required when the response is binary (application/octet-stream, etc.).
    pub output_file: Option<String>,
    /// Message to display for confirm steps
    pub message: Option<String>,
    /// "warn" | "confirm" | "fail" (default)
    pub on_failure: Option<String>,
    /// "always" | "on_success" (default)
    pub when: Option<String>,
    /// If true, failures are silently ignored
    pub optional: Option<bool>,
    /// Capture stdout into this variable name
    pub capture: Option<String>,
    pub poll: Option<PollConfig>,
    pub assert: Option<AssertConfig>,
}

/// Polling configuration for a step.
#[derive(Deserialize, Serialize, Clone)]
pub struct PollConfig {
    /// Poll interval: "30s" | "1m" | "5m"
    pub interval: String,
    /// Total timeout: "5m" | "1h"
    pub timeout: String,
    /// Condition to stop polling: "status == OK" | "value < 5" | "decreasing" | "empty"
    pub until: String,
}

/// Assertion configuration for a step.
#[derive(Deserialize, Serialize, Clone)]
pub struct AssertConfig {
    /// Assert that the output is empty
    pub empty: Option<bool>,
    /// Custom error message on assertion failure
    pub message: Option<String>,
}

/// Lightweight runbook metadata for `pup runbooks list`.
pub struct RunbookMeta {
    pub name: String,
    pub description: Option<String>,
    pub tags: HashMap<String, String>,
    pub steps: usize,
}
