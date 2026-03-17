use crate::config::Config;
/// ACP (Agent Communication Protocol) server that proxies to Bits AI assistant.
///
/// Starts a local HTTP server implementing ACP and delegates requests to
/// the Datadog Bits AI endpoint (/api/v2/assistant). Authentication uses
/// OAuth2 bearer token or API key + app key.
///
/// Protocol:  https://agentcommunicationprotocol.dev/
/// Endpoint:  GET  /agent.json       — agent card
///            POST /runs             — synchronous run
///            POST /runs/stream      — streaming run (SSE)
use anyhow::Result;

#[cfg(not(target_arch = "wasm32"))]
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedWriteHalf, TcpListener},
};

pub const DEFAULT_PORT: u16 = 9099;
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// Starts the ACP server on the given host and port.
#[cfg(not(target_arch = "wasm32"))]
pub async fn serve(cfg: &Config, port: u16, host: &str) -> Result<()> {
    cfg.validate_auth()?;

    let addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to {addr}: {e}"))?;

    let base_url = format!("http://{addr}");
    eprintln!("ACP server running at {base_url}");
    eprintln!("  Agent card:  GET  {base_url}/agent.json");
    eprintln!("  Sync run:    POST {base_url}/runs");
    eprintln!("  Stream run:  POST {base_url}/runs/stream");
    eprintln!("Press Ctrl+C to stop.");

    // Construct app base URL: https://app.<site> (e.g. app.datadoghq.com)
    let app_base = format!("https://app.{}", cfg.site);
    let access_token = cfg.access_token.clone();
    let api_key = cfg.api_key.clone();
    let app_key = cfg.app_key.clone();

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let app_base = app_base.clone();
        let access_token = access_token.clone();
        let api_key = api_key.clone();
        let app_key = app_key.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(
                stream,
                peer_addr,
                &app_base,
                access_token.as_deref(),
                api_key.as_deref(),
                app_key.as_deref(),
            )
            .await
            {
                eprintln!("[{peer_addr}] Error: {e}");
            }
        });
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn handle_connection(
    stream: tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
    app_base: &str,
    access_token: Option<&str>,
    api_key: Option<&str>,
    app_key: Option<&str>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Read request line
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;
    let request_line = request_line.trim();
    let parts: Vec<&str> = request_line.splitn(3, ' ').collect();
    if parts.len() < 2 {
        return Ok(());
    }
    let method = parts[0];
    let path = parts[1];

    eprintln!("[{peer_addr}] {method} {path}");

    // Read headers, capture Content-Length
    let mut content_length: usize = 0;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if let Some(val) = trimmed.to_lowercase().strip_prefix("content-length:") {
            content_length = val.trim().parse().unwrap_or(0);
        }
    }

    // Read body
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).await?;
    }

    match (method, path) {
        ("GET", "/agent.json") | ("GET", "/.well-known/agent.json") => {
            write_agent_card(&mut writer).await
        }
        ("POST", "/runs") => {
            handle_run(
                &mut writer,
                app_base,
                access_token,
                api_key,
                app_key,
                &body,
                false,
            )
            .await
        }
        ("POST", "/runs/stream") => {
            handle_run(
                &mut writer,
                app_base,
                access_token,
                api_key,
                app_key,
                &body,
                true,
            )
            .await
        }
        ("OPTIONS", _) => {
            // CORS preflight
            writer
                .write_all(
                    b"HTTP/1.1 204 No Content\r\n\
                      Access-Control-Allow-Origin: *\r\n\
                      Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
                      Access-Control-Allow-Headers: Content-Type\r\n\
                      \r\n",
                )
                .await?;
            Ok(())
        }
        _ => write_json_response(&mut writer, 404, serde_json::json!({"error": "not found"})).await,
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn write_agent_card(writer: &mut OwnedWriteHalf) -> Result<()> {
    let card = serde_json::json!({
        "name": "Bits AI",
        "description": "Datadog Bits AI assistant — answers questions about your Datadog environment, metrics, logs, monitors, and more.",
        "version": "1.0.0",
        "url": "",
        "capabilities": {
            "streaming": true
        },
        "metadata": {
            "provider": "Datadog",
            "endpoint": "/api/v2/assistant"
        }
    });
    write_json_response(writer, 200, card).await
}

/// Handles both sync (`POST /runs`) and streaming (`POST /runs/stream`) ACP requests.
#[cfg(not(target_arch = "wasm32"))]
async fn handle_run(
    writer: &mut OwnedWriteHalf,
    app_base: &str,
    access_token: Option<&str>,
    api_key: Option<&str>,
    app_key: Option<&str>,
    body: &[u8],
    streaming: bool,
) -> Result<()> {
    // Parse ACP request body
    let acp_req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return write_json_response(
                writer,
                400,
                serde_json::json!({"error": format!("invalid JSON: {e}")}),
            )
            .await;
        }
    };

    let message = match extract_acp_message(&acp_req) {
        Some(m) => m,
        None => {
            return write_json_response(
                writer,
                400,
                serde_json::json!({"error": "missing message content in input"}),
            )
            .await;
        }
    };

    // session_id maps to conversation_id for multi-turn conversations
    let conversation_id = acp_req
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let run_id = uuid::Uuid::new_v4().to_string();

    // Build Bits AI request payload
    let bits_body = serde_json::json!({
        "data": {
            "id": "assistant-request",
            "type": "assistant-request",
            "attributes": {
                "message": {
                    "type": "markdown_fragment",
                    "content": message
                },
                "conversation_id": conversation_id,
                "context": {"entities": []},
                "enable_debug_mode": false
            }
        }
    });

    // Send request to Bits AI
    let bits_url = format!("{app_base}/api/v2/assistant");
    let client = reqwest::Client::new();
    let mut req = client
        .post(&bits_url)
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream");

    if let Some(token) = access_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    } else if let (Some(ak), Some(apk)) = (api_key, app_key) {
        req = req
            .header("DD-API-KEY", ak)
            .header("DD-APPLICATION-KEY", apk);
    } else {
        return write_json_response(
            writer,
            401,
            serde_json::json!({"error": "no authentication configured"}),
        )
        .await;
    }

    let resp = match req.json(&bits_body).send().await {
        Ok(r) => r,
        Err(e) => {
            return write_json_response(
                writer,
                502,
                serde_json::json!({"error": format!("upstream request failed: {e}")}),
            )
            .await;
        }
    };

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let err_body = resp.text().await.unwrap_or_default();
        return write_json_response(
            writer,
            status,
            serde_json::json!({"error": format!("Bits AI error (HTTP {status}): {err_body}")}),
        )
        .await;
    }

    if streaming {
        stream_bits_ai_to_acp(writer, resp, &run_id).await
    } else {
        collect_bits_ai_to_acp(writer, resp, &run_id).await
    }
}

/// Collects the full Bits AI SSE stream and returns a single ACP run response.
#[cfg(not(target_arch = "wasm32"))]
async fn collect_bits_ai_to_acp(
    writer: &mut OwnedWriteHalf,
    resp: reqwest::Response,
    run_id: &str,
) -> Result<()> {
    let (text, session_id) = collect_markdown_from_stream(resp).await?;

    let output = serde_json::json!([{
        "role": "assistant",
        "content": [{"type": "text", "text": text}]
    }]);

    let body = serde_json::json!({
        "run_id": run_id,
        "agent_id": "bits-ai",
        "session_id": session_id,
        "status": "completed",
        "output": output
    });
    write_json_response(writer, 200, body).await
}

/// Streams Bits AI SSE output as ACP SSE events.
#[cfg(not(target_arch = "wasm32"))]
async fn stream_bits_ai_to_acp(
    writer: &mut OwnedWriteHalf,
    resp: reqwest::Response,
    run_id: &str,
) -> Result<()> {
    use futures::StreamExt;

    // Write SSE response headers
    writer
        .write_all(
            b"HTTP/1.1 200 OK\r\n\
              Content-Type: text/event-stream\r\n\
              Cache-Control: no-cache\r\n\
              X-Accel-Buffering: no\r\n\
              Access-Control-Allow-Origin: *\r\n\
              \r\n",
        )
        .await?;

    // Send run_created event
    write_sse_event(
        writer,
        "run_created",
        &serde_json::json!({
            "type": "run_created",
            "run_id": run_id,
            "agent_id": "bits-ai",
            "status": "running"
        }),
    )
    .await?;

    let mut buffer = String::new();
    let mut session_id = String::new();
    let mut message_id: Option<String> = None;
    let mut message_started = false;
    let mut full_text = String::new();
    let mut bytes_stream = resp.bytes_stream();

    while let Some(chunk_result) = bytes_stream.next().await {
        let chunk = chunk_result.map_err(|e| anyhow::anyhow!("stream read error: {e}"))?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        // Process complete SSE event blocks (separated by double newline)
        while let Some(end) = buffer.find("\n\n") {
            let event_block = buffer[..end].to_string();
            buffer = buffer[end + 2..].to_string();

            for line in event_block.lines() {
                let Some(data_str) = line.strip_prefix("data: ") else {
                    continue;
                };
                let Ok(val) = serde_json::from_str::<serde_json::Value>(data_str) else {
                    continue;
                };

                // Skip initial system-prompt event (has "prompt" key, no structured_message)
                let Some(attrs) = val.pointer("/data/attributes") else {
                    continue;
                };
                let Some(sm) = attrs.get("structured_message") else {
                    continue;
                };

                // Capture conversation_id for session_id
                if session_id.is_empty() {
                    if let Some(cid) = attrs.get("conversation_id").and_then(|v| v.as_str()) {
                        session_id = cid.to_string();
                    }
                }

                // Capture message_id for message_start event
                if message_id.is_none() {
                    if let Some(mid) = sm.get("message_id").and_then(|v| v.as_str()) {
                        message_id = Some(mid.to_string());
                    }
                }

                let content = match sm.get("content") {
                    Some(c) => c,
                    None => continue,
                };

                let content_type = content.get("type").and_then(|v| v.as_str()).unwrap_or("");

                // Only emit markdown_fragment content (skip thinking tokens)
                if content_type != "markdown_fragment" {
                    continue;
                }

                let fragment = content
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Send message_start before the first chunk
                if !message_started && !fragment.is_empty() {
                    message_started = true;
                    write_sse_event(
                        writer,
                        "message_created",
                        &serde_json::json!({
                            "type": "message_created",
                            "run_id": run_id,
                            "message_id": message_id,
                            "role": "assistant"
                        }),
                    )
                    .await?;
                }

                if !fragment.is_empty() {
                    full_text.push_str(fragment);
                    write_sse_event(
                        writer,
                        "message_chunk",
                        &serde_json::json!({
                            "type": "message_chunk",
                            "run_id": run_id,
                            "delta": {
                                "role": "assistant",
                                "content": [{"type": "text", "text": fragment}]
                            }
                        }),
                    )
                    .await?;
                }

                // End-of-stream: empty fragment with results.usage present
                if fragment.is_empty() && sm.get("results").is_some() {
                    // message_completed
                    write_sse_event(
                        writer,
                        "message_completed",
                        &serde_json::json!({
                            "type": "message_completed",
                            "run_id": run_id,
                            "message": {
                                "role": "assistant",
                                "content": [{"type": "text", "text": &full_text}]
                            }
                        }),
                    )
                    .await?;

                    // run_completed
                    write_sse_event(
                        writer,
                        "run_completed",
                        &serde_json::json!({
                            "type": "run_completed",
                            "run_id": run_id,
                            "agent_id": "bits-ai",
                            "session_id": &session_id,
                            "status": "completed",
                            "output": [{
                                "role": "assistant",
                                "content": [{"type": "text", "text": &full_text}]
                            }]
                        }),
                    )
                    .await?;

                    return Ok(());
                }
            }
        }
    }

    // Stream ended without a clean end-of-stream marker — still emit completion
    if message_started {
        write_sse_event(
            writer,
            "run_completed",
            &serde_json::json!({
                "type": "run_completed",
                "run_id": run_id,
                "agent_id": "bits-ai",
                "session_id": &session_id,
                "status": "completed",
                "output": [{
                    "role": "assistant",
                    "content": [{"type": "text", "text": &full_text}]
                }]
            }),
        )
        .await?;
    }

    Ok(())
}

/// Reads a Bits AI SSE stream to completion, returning the assembled text and session_id.
#[cfg(not(target_arch = "wasm32"))]
async fn collect_markdown_from_stream(resp: reqwest::Response) -> Result<(String, String)> {
    use futures::StreamExt;

    let mut buffer = String::new();
    let mut full_text = String::new();
    let mut session_id = String::new();
    let mut bytes_stream = resp.bytes_stream();

    while let Some(chunk_result) = bytes_stream.next().await {
        let chunk = chunk_result.map_err(|e| anyhow::anyhow!("stream read error: {e}"))?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(end) = buffer.find("\n\n") {
            let event_block = buffer[..end].to_string();
            buffer = buffer[end + 2..].to_string();

            for line in event_block.lines() {
                let Some(data_str) = line.strip_prefix("data: ") else {
                    continue;
                };
                let Ok(val) = serde_json::from_str::<serde_json::Value>(data_str) else {
                    continue;
                };

                let Some(attrs) = val.pointer("/data/attributes") else {
                    continue;
                };
                let Some(sm) = attrs.get("structured_message") else {
                    continue;
                };

                if session_id.is_empty() {
                    if let Some(cid) = attrs.get("conversation_id").and_then(|v| v.as_str()) {
                        session_id = cid.to_string();
                    }
                }

                let Some(content) = sm.get("content") else {
                    continue;
                };
                if content.get("type").and_then(|v| v.as_str()) != Some("markdown_fragment") {
                    continue;
                }

                let fragment = content
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                full_text.push_str(fragment);
            }
        }
    }

    Ok((full_text, session_id))
}

/// Extracts the first text content from an ACP input messages array.
fn extract_acp_message(req: &serde_json::Value) -> Option<String> {
    let input = req.get("input")?.as_array()?;
    for msg in input {
        if msg.get("role").and_then(|r| r.as_str()) != Some("user") {
            continue;
        }
        if let Some(content) = msg.get("content") {
            // content can be a string or an array of content blocks
            if let Some(s) = content.as_str() {
                if !s.is_empty() {
                    return Some(s.to_string());
                }
            } else if let Some(blocks) = content.as_array() {
                for block in blocks {
                    if block.get("type").and_then(|t| t.as_str()) == Some("text") {
                        if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                            if !text.is_empty() {
                                return Some(text.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
async fn write_json_response(
    writer: &mut OwnedWriteHalf,
    status: u16,
    body: serde_json::Value,
) -> Result<()> {
    let body_bytes = body.to_string();
    let reason = http_reason(status);
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         \r\n\
         {body_bytes}",
        body_bytes.len()
    );
    writer.write_all(response.as_bytes()).await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn write_sse_event(
    writer: &mut OwnedWriteHalf,
    event_type: &str,
    data: &serde_json::Value,
) -> Result<()> {
    let data_str = data.to_string();
    let event = format!("event: {event_type}\ndata: {data_str}\n\n");
    writer.write_all(event.as_bytes()).await?;
    Ok(())
}

fn http_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        502 => "Bad Gateway",
        _ => "Internal Server Error",
    }
}

// ---------------------------------------------------------------------------
// WASM stub
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
pub async fn serve(_cfg: &Config, _port: u16, _host: &str) -> Result<()> {
    anyhow::bail!("acp serve is not supported in WASM")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_acp_message_text_block() {
        let req = serde_json::json!({
            "input": [{
                "role": "user",
                "content": [{"type": "text", "text": "hello world"}]
            }]
        });
        assert_eq!(extract_acp_message(&req), Some("hello world".to_string()));
    }

    #[test]
    fn test_extract_acp_message_string_content() {
        let req = serde_json::json!({
            "input": [{
                "role": "user",
                "content": "simple string message"
            }]
        });
        assert_eq!(
            extract_acp_message(&req),
            Some("simple string message".to_string())
        );
    }

    #[test]
    fn test_extract_acp_message_skips_non_user() {
        let req = serde_json::json!({
            "input": [
                {"role": "system", "content": [{"type": "text", "text": "system prompt"}]},
                {"role": "user", "content": [{"type": "text", "text": "user msg"}]}
            ]
        });
        assert_eq!(extract_acp_message(&req), Some("user msg".to_string()));
    }

    #[test]
    fn test_extract_acp_message_missing_input() {
        let req = serde_json::json!({"agent_id": "bits-ai"});
        assert_eq!(extract_acp_message(&req), None);
    }

    #[test]
    fn test_http_reason() {
        assert_eq!(http_reason(200), "OK");
        assert_eq!(http_reason(400), "Bad Request");
        assert_eq!(http_reason(404), "Not Found");
        assert_eq!(http_reason(401), "Unauthorized");
        assert_eq!(http_reason(502), "Bad Gateway");
    }
}
