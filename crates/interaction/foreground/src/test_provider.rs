//! Loopback-only HTTP barrier shared by foreground and daemon acceptance tests.
use std::sync::Arc;
use tachyon_model::{Model, ModelConfig};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

pub(crate) struct LocalProvider {
    pub model: Arc<Model>,
    #[allow(dead_code)] // Used by the daemon's subprocess fixture, not in-process tests.
    pub endpoint: String,
    pub requests: mpsc::UnboundedReceiver<Request>,
    server: tokio::task::JoinHandle<()>,
}

pub(crate) struct Request {
    pub body: serde_json::Value,
    stream: TcpStream,
}

impl LocalProvider {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let model = Arc::new(Model::new(ModelConfig {
            base_url: endpoint.clone(),
            api_key: "local-test-only".into(),
            model: "scripted-test-model".into(),
            temperature: 0.0,
            max_completion_tokens: Some(128),
            context_length: Some(4096),
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: None,
            debug: false,
            debug_log: None,
        }));
        let (tx, requests) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                assert_eq!(line, "POST /chat/completions HTTP/1.1\r\n");
                let mut length = None;
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            length = Some(value.trim().parse::<usize>().unwrap());
                        }
                    }
                }
                let length = length.expect("model request must have a content length");
                assert!(length < 1024 * 1024);
                let mut body = vec![0; length];
                reader.read_exact(&mut body).await.unwrap();
                let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(body["model"], "scripted-test-model");
                assert_eq!(body["stream"], true);
                // Owning this socket is the barrier, not a timing assumption.
                if tx
                    .send(Request {
                        body,
                        stream: reader.into_inner(),
                    })
                    .is_err()
                {
                    return;
                }
            }
        });
        Self {
            model,
            endpoint,
            requests,
            server,
        }
    }

    pub async fn shutdown(&mut self) {
        self.server.abort();
        let result = (&mut self.server).await;
        assert!(result.is_ok() || result.unwrap_err().is_cancelled());
    }
}

impl Drop for LocalProvider {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Request {
    pub async fn start_stream(&mut self) {
        self.stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
    }

    pub async fn send_delta(&mut self, delta: serde_json::Value) {
        self.try_send_delta(delta).await.unwrap();
    }

    pub async fn try_send_delta(&mut self, delta: serde_json::Value) -> std::io::Result<()> {
        let event = serde_json::json!({"choices": [{"index": 0, "delta": delta}]});
        self.stream
            .write_all(format!("data: {event}\n\n").as_bytes())
            .await
    }

    pub async fn finish_completion(mut self, reason: &str) {
        let event =
            serde_json::json!({"choices": [{"index": 0, "delta": {}, "finish_reason": reason}]});
        self.stream
            .write_all(format!("data: {event}\n\n").as_bytes())
            .await
            .unwrap();
        self.finish_stream().await;
    }

    pub async fn start_truncated_stream(&mut self) {
        self.stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n").await.unwrap();
    }

    pub async fn fail_stream(mut self, malformed: bool) {
        let body = if malformed {
            "not-json"
        } else {
            r#"{"error":{"message":"private provider failure"}}"#
        };
        let _ = self
            .stream
            .write_all(format!("data: {body}\n\n").as_bytes())
            .await;
    }

    pub async fn finish_stream(mut self) {
        self.stream.write_all(b"data: [DONE]\n\n").await.unwrap();
    }

    pub async fn respond_error(mut self) {
        self.stream
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
    }

    pub async fn respond_tool(self, name: &str, arguments: serde_json::Value) {
        self.respond_tool_text(name, arguments, "").await;
    }

    pub async fn respond_tool_text(mut self, name: &str, arguments: serde_json::Value, text: &str) {
        assert!(
            self.body["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["function"]["name"] == name),
            "unadvertised tool {name}"
        );
        let call = self.body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message["role"] == "tool")
            .count();
        let body = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            serde_json::json!({
                "choices":[{"index":0,"delta":{"content":text,"tool_calls":[{
                    "index":0,"id":format!("scripted-call-{call}"),"type":"function",
                    "function":{"name":name,"arguments":arguments.to_string()}
                }]},"finish_reason":"tool_calls"}],
                "usage":{"prompt_tokens":12,"completion_tokens":4,"total_tokens":16,"cost":0.000016}
            })
        );
        let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
        self.stream.write_all(headers.as_bytes()).await.unwrap();
        self.stream.write_all(body.as_bytes()).await.unwrap();
    }

    pub async fn respond(mut self, deltas: &[&str]) {
        let mut body = String::new();
        for delta in deltas {
            body.push_str(&format!(
                "data: {}\n\n",
                serde_json::json!({
                    "choices": [{"index": 0, "delta": {"content": delta}, "finish_reason": null}]
                })
            ));
        }
        body.push_str("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":4,\"total_tokens\":16,\"cost\":0.000016}}\n\ndata: [DONE]\n\n");
        let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
        self.stream.write_all(headers.as_bytes()).await.unwrap();
        for chunk in body.as_bytes().chunks(7) {
            self.stream.write_all(chunk).await.unwrap();
            self.stream.flush().await.unwrap();
        }
    }
}
