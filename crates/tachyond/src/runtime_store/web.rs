//! Durable per-root escrow. No transaction survives provider I/O.
use super::*;
use tachyon_api::web::{WebCommand, WebResult};
use tachyon_model::accounting::RequestUsage;
use tachyon_util::config::WebPolicy;

const TURNS: TableDefinition<&str, &[u8]> = TableDefinition::new("web_turns");

#[derive(Serialize, Deserialize)]
struct Turn {
    model: String,
    policy: WebPolicy,
    requests: Vec<Record>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::web::{WebRequest, WebStatus};
    fn command(id: &str) -> WebCommand {
        WebCommand {
            command_id: id.into(),
            request_id: id.into(),
            caller_id: "foreground".into(),
            tool_call_id: id.into(),
            turn_id: "turn".into(),
            request: WebRequest::Search {
                query: "rust".into(),
                domains: None,
                max_results: 3,
            },
        }
    }
    fn report() -> WebResult {
        WebResult {
            usage: Default::default(),
            answer: "report".into(),
            citations: vec![],
            annotations: vec![],
            status: WebStatus::Unverified,
            notice: "report not raw pages".into(),
            host_observed_at: 1,
            observed_search_uses: Some(1),
            observed_fetch_uses: None,
            requested_urls: vec![],
        }
    }
    const FINAL: RequestUsage = RequestUsage::Final {
        input_tokens: 10,
        output_tokens: 2,
        cost_micro_usd: 7000,
    };

    #[test]
    fn durable_replay_cap_unknown_overclaim_and_independent_roots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let policy = WebPolicy {
            max_requests: 2,
            ..Default::default()
        };
        let first = command("one");
        assert!(store
            .web_reserve("root", "model", &policy, &first, 1)
            .unwrap()
            .is_none());
        assert!(store
            .web_reserve("root", "model", &policy, &first, 1)
            .is_err());
        assert!(store
            .web_reserve("root", "model", &policy, &command("two"), 1)
            .is_err());
        store
            .web_finish("root", &first, FINAL, Ok(report()))
            .unwrap()
            .unwrap();
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(
            store
                .web_reserve("root", "model", &policy, &first, 1)
                .unwrap()
                .unwrap()
                .unwrap()
                .answer,
            "report"
        );
        let mut conflict = first.clone();
        conflict.request = WebRequest::Search {
            query: "different".into(),
            domains: None,
            max_results: 3,
        };
        assert!(store
            .web_reserve("root", "model", &policy, &conflict, 1)
            .is_err());
        let second = command("two");
        store
            .web_reserve("root", "model", &policy, &second, 1)
            .unwrap();
        store
            .web_finish("root", &second, FINAL, Ok(report()))
            .unwrap()
            .unwrap();
        assert!(store
            .web_reserve("root", "model", &policy, &command("three"), 1)
            .is_err());
        store
            .web_reserve("unknown", "model", &policy, &first, 1)
            .unwrap();
        store
            .web_finish("unknown", &first, RequestUsage::Unknown, Ok(report()))
            .unwrap()
            .unwrap();
        assert!(store
            .web_reserve("unknown", "model", &policy, &second, 1)
            .is_err());
        assert!(store
            .web_reserve("fresh", "model", &policy, &second, 1)
            .is_ok());
        store
            .web_reserve("overclaim", "model", &policy, &first, 1)
            .unwrap();
        let mut over = report();
        over.observed_fetch_uses = Some(1);
        store
            .web_finish("overclaim", &first, FINAL, Ok(over))
            .unwrap()
            .unwrap();
        assert!(store
            .web_reserve("overclaim", "model", &policy, &second, 1)
            .is_err());
        store
            .web_reserve("cost-over", "model", &policy, &first, 1)
            .unwrap();
        store
            .web_finish(
                "cost-over",
                &first,
                RequestUsage::Final {
                    input_tokens: 10,
                    output_tokens: 2,
                    cost_micro_usd: 250001,
                },
                Ok(report()),
            )
            .unwrap()
            .unwrap();
        assert!(store
            .web_reserve("cost-over", "model", &policy, &second, 1)
            .is_err());
        let mut four = command("four");
        four.request = WebRequest::Fetch {
            urls: vec!["https://example.com".into(); 4],
            instruction: None,
            follow_links: None,
        };
        store
            .web_reserve("all-uses", "model", &policy, &four, 4)
            .unwrap();
        store
            .web_finish("all-uses", &four, FINAL, Ok(report()))
            .unwrap()
            .unwrap();
        assert!(store
            .web_reserve("all-uses", "model", &policy, &second, 1)
            .is_err());
        assert!(store
            .web_reserve(
                "root",
                "model",
                &WebPolicy::default(),
                &command("upgrade"),
                1
            )
            .is_err());
    }

    #[test]
    fn concurrent_claim_is_exclusive_and_integer_budgets_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            std::sync::Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                std::thread::spawn(move || {
                    store
                        .web_reserve("same", "model", &WebPolicy::default(), &command("one"), 1)
                        .is_ok()
                })
            })
            .collect();
        assert_eq!(
            threads
                .into_iter()
                .filter_map(|t| t.join().ok())
                .filter(|ok| *ok)
                .count(),
            1
        );
        let policy = WebPolicy {
            turn_cost_micro_usd: 249999,
            ..Default::default()
        };
        assert!(store
            .web_reserve("cost", "model", &policy, &command("one"), 1)
            .is_err());
        let policy = WebPolicy {
            turn_tokens: 67583,
            ..Default::default()
        };
        assert!(store
            .web_reserve("tokens", "model", &policy, &command("one"), 1)
            .is_err());
        let policy = WebPolicy {
            input_micro_usd_per_million: Some(u64::MAX),
            output_micro_usd_per_million: Some(u64::MAX),
            ..Default::default()
        };
        assert!(store
            .web_reserve("priced", "model", &policy, &command("one"), 1)
            .is_err());
    }
}

#[derive(Serialize, Deserialize)]
struct Record {
    command: WebCommand,
    calls: u8,
    tokens: u64,
    cost: u64,
    usage: RequestUsage,
    result: Option<Result<WebResult, String>>,
    blocked: bool,
}

impl RuntimeStore {
    /// None claims exactly one dispatch. A completed duplicate replays its result.
    pub(crate) fn web_reserve(
        &self,
        scope: &str,
        model: &str,
        policy: &WebPolicy,
        command: &WebCommand,
        calls: u8,
    ) -> Result<Option<Result<WebResult, String>>, String> {
        command.validate()?;
        let expected_calls = match &command.request {
            tachyon_api::web::WebRequest::Search { .. } => 1,
            tachyon_api::web::WebRequest::Fetch { urls, .. } => urls.len() as u8,
        };
        if calls != expected_calls || calls > policy.max_fetch_urls {
            return Err("web server-call reservation mismatch".into());
        }
        let (tokens, cost) = policy.reservation(calls)?;
        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
        let mut table = tx.open_table(TURNS).map_err(|e| e.to_string())?;
        let mut turn = table
            .get(scope)
            .map_err(|e| e.to_string())?
            .map(|v| serde_json::from_slice::<Turn>(v.value()))
            .transpose()
            .map_err(|e| e.to_string())?
            .unwrap_or_else(|| Turn {
                model: model.into(),
                policy: policy.clone(),
                requests: vec![],
            });
        if turn.model != model || turn.policy != *policy {
            return Err("web root policy changed".into());
        }
        if let Some(record) = turn
            .requests
            .iter()
            .find(|r| r.command.request_id == command.request_id)
        {
            if record.command != *command {
                return Err("web request identity conflict".into());
            }
            return record
                .result
                .clone()
                .map(Some)
                .ok_or_else(|| "web request in flight or outcome unknown; no redispatch".into());
        }
        if turn
            .requests
            .iter()
            .any(|r| r.blocked || r.usage == RequestUsage::Unknown)
        {
            return Err("web root has unresolved spend or exceeded bounds".into());
        }
        let mut total_tokens = u128::from(tokens);
        let mut total_cost = u128::from(cost);
        let mut total_calls = u64::from(calls);
        for record in &turn.requests {
            total_calls += u64::from(record.calls);
            match record.usage {
                RequestUsage::Final {
                    input_tokens,
                    output_tokens,
                    cost_micro_usd,
                } => {
                    total_tokens += u128::from(input_tokens) + u128::from(output_tokens);
                    total_cost += u128::from(cost_micro_usd);
                }
                RequestUsage::Unknown => {
                    total_tokens += u128::from(record.tokens);
                    total_cost += u128::from(record.cost);
                }
            }
        }
        if turn.requests.len() >= usize::from(policy.max_requests)
            || total_calls > u64::from(policy.max_server_calls)
            || total_tokens > u128::from(policy.turn_tokens)
            || total_cost > u128::from(policy.turn_cost_micro_usd)
        {
            return Err("web root allowance exhausted".into());
        }
        turn.requests.push(Record {
            command: command.clone(),
            calls,
            tokens,
            cost,
            usage: RequestUsage::Unknown,
            result: None,
            blocked: false,
        });
        table
            .insert(
                scope,
                serde_json::to_vec(&turn)
                    .map_err(|e| e.to_string())?
                    .as_slice(),
            )
            .map_err(|e| e.to_string())?;
        drop(table);
        tx.commit().map_err(|e| e.to_string())?;
        Ok(None)
    }

    pub(crate) fn web_finish(
        &self,
        scope: &str,
        command: &WebCommand,
        usage: RequestUsage,
        mut result: Result<WebResult, String>,
    ) -> Result<Result<WebResult, String>, String> {
        if let Ok(report) = &mut result {
            use sha2::{Digest, Sha256};
            match usage {
                RequestUsage::Final { .. } => report.usage = usage.into(),
                RequestUsage::Unknown => report.usage.cost_micro_usd = None,
            }
            report.usage.receipt_id = Some(format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&(scope, &command.request_id)).map_err(|e| e.to_string())?
                )
            ));
        }
        if result.as_ref().is_ok_and(|report| {
            !report.usage.valid()
                || serde_json::to_vec(report).map_or(true, |bytes| bytes.len() > 262144)
        }) {
            result = Err("web report exceeds host result bounds".into());
        }
        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
        let mut table = tx.open_table(TURNS).map_err(|e| e.to_string())?;
        let mut turn: Turn = serde_json::from_slice(
            table
                .get(scope)
                .map_err(|e| e.to_string())?
                .ok_or("missing web root")?
                .value(),
        )
        .map_err(|e| e.to_string())?;
        let record = turn
            .requests
            .iter_mut()
            .find(|r| r.command == *command)
            .ok_or("missing web claim")?;
        if record.result.is_some() {
            return Err("web claim already completed".into());
        }
        record.usage = usage;
        if let RequestUsage::Final {
            input_tokens,
            output_tokens,
            cost_micro_usd,
        } = usage
        {
            record.blocked = input_tokens > turn.policy.input_tokens
                || output_tokens > u64::from(turn.policy.output_tokens)
                || cost_micro_usd > record.cost;
        }
        if let Ok(report) = &result {
            let uses = report
                .observed_search_uses
                .unwrap_or(0)
                .saturating_add(report.observed_fetch_uses.unwrap_or(0));
            record.blocked |= uses > u64::from(record.calls);
        }
        record.result = Some(result.clone());
        table
            .insert(
                scope,
                serde_json::to_vec(&turn)
                    .map_err(|e| e.to_string())?
                    .as_slice(),
            )
            .map_err(|e| e.to_string())?;
        drop(table);
        tx.commit().map_err(|e| e.to_string())?;
        Ok(result)
    }
}
