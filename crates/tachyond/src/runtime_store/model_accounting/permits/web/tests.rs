use super::*;
use serde_json::{json, Value};
use tachyon_api::{campaign::CampaignWeb, web::WebCommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn command(id: &str) -> WebCommand {
    WebCommand {
        command_id: id.into(),
        caller_id: "work".into(),
        tool_call_id: id.into(),
        turn_id: "turn".into(),
        request_id: id.into(),
        request: WebRequest::Search {
            query: "test".into(),
            domains: None,
            max_results: 3,
        },
    }
}

#[tokio::test]
async fn private_web_debits_work_replays_and_denies_revoked_unfunded_or_forged() {
    let (_dir, store, funding, mut binding) = super::super::tests::setup_web();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    binding.estimate.base_url = format!("http://{}", listener.local_addr().unwrap());
    binding.estimate.input_tokens = 65536;
    binding.estimate.output_tokens = 2048;
    binding.estimate.max_request_bytes = 32768;
    binding.estimate.other_micro_usd = 10000;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = vec![];
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
            }
            let header = String::from_utf8(header).unwrap();
            let len: usize = header
                .lines()
                .find_map(|line| {
                    let (k, v) = line.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse().unwrap())
                })
                .unwrap();
            let mut bytes = vec![0; len];
            socket.read_exact(&mut bytes).await.unwrap();
            tx.send(serde_json::from_slice::<Value>(&bytes).unwrap())
                .unwrap();
            let body = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"delta":{"content":"report"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12,"cost":0.007001,"server_tool_use":{"web_search_requests":1}}})
            );
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
    });
    let store = Arc::new(store);
    let permit = store
        .host_issue_model_permit(binding.clone(), funding.clone(), None)
        .unwrap();
    let model = super::super::broker_tests::model(&binding);
    let grant = CampaignWeb {
        max_requests: 1,
        max_server_calls: 2,
        inference_provider: "fake".into(),
    };
    let broker = ModelBroker::new(store.clone(), model)
        .with_web(grant.clone(), WebPolicy::default())
        .unwrap();
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    let (channel, client) = tachyon_model::broker::private_pair().unwrap();
    let host = broker.serve_private(channel, &permit, binding.clone(), deadline);
    let guest = async {
        let first = command("one");
        assert_eq!(
            client.web_lookup(first.clone()).await.unwrap().answer,
            "report"
        );
        assert_eq!(client.web_lookup(first).await.unwrap().answer, "report");
        let mut wrong = command("forged");
        wrong.caller_id = "other-work".into();
        assert!(client.web_lookup(wrong).await.is_err());
        let mut overfee = command("fees");
        overfee.request = WebRequest::Fetch {
            urls: vec![
                "https://example.com/a".into(),
                "https://example.com/b".into(),
            ],
            instruction: None,
            follow_links: None,
        };
        assert!(client.web_lookup(overfee).await.is_err());
        let mut fresh_root = command("cheap-final-new-root");
        fresh_root.turn_id = "worker-chosen-root".into();
        assert!(client.web_lookup(fresh_root).await.is_err());
        let mut stale = binding.clone();
        stale.identity.generation += 1;
        assert!(matches!(
            broker
                .web_private(
                    &permit,
                    &stale,
                    Request::WebSearch {
                        command: command("stale-generation")
                    },
                    deadline
                )
                .await,
            Reply::Denied
        ));
        store.host_revoke_model_permit(&permit).unwrap();
        assert!(client.web_lookup(command("revoked")).await.is_err());
        drop(client);
    };
    let (_, _) = tokio::join!(host, guest);
    let wire = rx.try_recv().unwrap();
    assert_eq!(wire["provider"]["only"], json!(["fake"]));
    assert_eq!(wire["tools"][0]["parameters"]["max_uses"], 1);
    assert!(rx.try_recv().is_err());
    let read = store.database.begin_read().unwrap();
    let table = read.open_table(DISPATCHES).unwrap();
    let records: Vec<DispatchRecord> = table
        .iter()
        .unwrap()
        .map(|r| serde_json::from_slice(r.unwrap().1.value()).unwrap())
        .collect();
    assert_eq!(records.len(), 1);
    let ledger = store
        .campaign_ledger(&binding.identity.campaign_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        ledger.reservations[&records[0].receipt].usage,
        Usage::Final(Units {
            tokens: 12,
            cost_micro_usd: 7001
        })
    );
    let model = super::super::broker_tests::model(&binding);
    let no_grant = ModelBroker::new(store.clone(), model).with_controls([Control::WebSearch]);
    assert!(matches!(
        no_grant
            .web_private(
                &permit,
                &binding,
                Request::WebSearch {
                    command: command("no-grant")
                },
                deadline
            )
            .await,
        Reply::Denied
    ));
    server.abort();
}
