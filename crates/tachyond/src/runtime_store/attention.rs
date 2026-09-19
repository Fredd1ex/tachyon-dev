//! Host-only admission and durable notification outbox. No log text is authority.
//! Ordinary turn-owned failures use the retained WorkResult route, not a second
//! user notice. Campaign failures and explicit questions remain attention-worthy.
use super::{operational_events as feed, RuntimeStore};
use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use tachyon_api::{attention::*, operational_events::*, todo::TodoScope};

const RECORDS: TableDefinition<&str, &[u8]> = TableDefinition::new("attention_v1");
const CAUSES: TableDefinition<&str, &str> = TableDefinition::new("attention_causes_v1");
const SCOPED: TableDefinition<(&str, &str), ()> = TableDefinition::new("attention_scope_index_v1");
const PENDING: TableDefinition<(u8, &str), ()> = TableDefinition::new("attention_pending_v1");
const FRAMES: TableDefinition<&str, &[u8]> = TableDefinition::new("attention_frames_v1");
const ACTIVE_FRAMES: TableDefinition<&str, ()> = TableDefinition::new("attention_active_frames_v1");

pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    tx.open_table(RECORDS).map_err(err)?;
    tx.open_table(CAUSES).map_err(err)?;
    tx.open_table(SCOPED).map_err(err)?;
    tx.open_table(PENDING).map_err(err)?;
    tx.open_table(FRAMES).map_err(err)?;
    tx.open_table(ACTIVE_FRAMES).map_err(err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::{
        ApiRequest, ApiResponse, InteractionEvent, InteractionEventEnvelope, InteractionMetadata,
    };

    fn scope() -> TodoScope {
        TodoScope::Conversation {
            id: "foreground".into(),
        }
    }
    fn source(work: &str) -> AttentionSource {
        AttentionSource {
            scope: scope(),
            work_id: Some(work.into()),
            campaign_id: None,
            generation: 1,
            instruction_revision: 0,
            category: AttentionCategory::WorkFailed,
            cause_id: "terminal:1".into(),
        }
    }
    fn admit(store: &RuntimeStore, work: &str) -> Attention {
        let tx = store.database.begin_write().unwrap();
        let record = RuntimeStore::admit_attention_in(&tx, source(work), 100).unwrap();
        tx.commit().unwrap();
        record
    }
    fn publication(frame: &AttentionFrame) -> InteractionEventEnvelope {
        let mut metadata = InteractionMetadata::new(
            format!("{}:published", frame.command_id),
            &frame.command_id,
            "foreground",
            200,
        );
        metadata.causation_id = Some(frame.command_id.clone());
        InteractionEventEnvelope {
            metadata,
            event: InteractionEvent::UserVisibleNotificationPublished {
                text: frame.text.clone(),
            },
        }
    }

    #[test]
    fn crash_before_notify_replays_and_all_phases_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let id;
        let initial;
        {
            let store = RuntimeStore::open(&path).unwrap();
            initial = store
                .attention_snapshot(&scope(), None, 10)
                .unwrap()
                .watermark;
            let record = admit(&store, "work");
            assert_eq!(record, admit(&store, "work"));
            id = record.id;
            assert_eq!(
                store
                    .attention_snapshot(&scope(), None, 10)
                    .unwrap()
                    .watermark
                    .sequence,
                initial.sequence + 1
            );
        }
        let store = RuntimeStore::open(&path).unwrap();
        let frame = store.claim_attention_frame(200).unwrap().unwrap();
        assert!(store.claim_attention_frame(201).unwrap().is_none());
        let retry = store.claim_attention_frame(5200).unwrap().unwrap();
        assert_eq!(frame.command_id, retry.command_id);
        assert_eq!(frame.ids, retry.ids);
        assert!(store
            .acknowledge_attention(&scope(), &id, AttentionAcknowledgement::Displayed, 202)
            .is_err());
        let mut event = publication(&frame);
        assert!(store.admit_attention_publication(&mut event, 5300).unwrap());
        let delivered = store.attention_snapshot(&scope(), None, 10).unwrap();
        assert_eq!(delivered.records[0].delivered_at_ms, Some(5300));
        assert_eq!(delivered.records[0].displayed_at_ms, None);
        assert_eq!(store.pending_history().unwrap().len(), 1);
        store.admit_attention_publication(&mut event, 5400).unwrap();
        assert_eq!(
            store
                .attention_snapshot(&scope(), None, 10)
                .unwrap()
                .watermark,
            delivered.watermark
        );
        assert!(store.claim_attention_frame(10_000).unwrap().is_none());
        let wrong = TodoScope::Conversation { id: "other".into() };
        assert!(store
            .acknowledge_attention(&wrong, &id, AttentionAcknowledgement::Acknowledged, 5500)
            .is_err());
        store
            .acknowledge_attention(&scope(), &id, AttentionAcknowledgement::Displayed, 5500)
            .unwrap();
        let record = store
            .acknowledge_attention(&scope(), &id, AttentionAcknowledgement::Acknowledged, 5600)
            .unwrap();
        assert_eq!(
            record,
            store
                .acknowledge_attention(&scope(), &id, AttentionAcknowledgement::Acknowledged, 5700)
                .unwrap()
        );
        let feed = store
            .todos(super::super::todo::TodoAuthority::Bound {
                scope: scope(),
                actor: tachyon_api::todo::TodoActor {
                    source: "operator".into(),
                    actor: "test".into(),
                },
            })
            .unwrap();
        let replay = feed.operational_batch(&initial).unwrap();
        assert_eq!(replay.events.len(), 4);
        drop(feed);
        drop(store);
        let reopened = RuntimeStore::open(&path).unwrap();
        assert_eq!(
            reopened
                .attention_snapshot(&scope(), None, 10)
                .unwrap()
                .records,
            vec![record]
        );
    }

    #[test]
    fn rollback_and_absent_ui_keep_records_and_bound_frames() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let initial = store.attention_snapshot(&scope(), None, 100).unwrap();
        {
            let tx = store.database.begin_write().unwrap();
            RuntimeStore::admit_attention_in(&tx, source("rolled-back"), 1).unwrap();
        }
        assert_eq!(
            initial,
            store.attention_snapshot(&scope(), None, 100).unwrap()
        );
        for i in 0..300 {
            admit(&store, &format!("work-{i}"));
        }
        let mut frames = Vec::new();
        for _ in 0..8 {
            frames.push(store.claim_attention_frame(1000).unwrap().unwrap());
        }
        assert!(store.claim_attention_frame(1000).unwrap().is_none());
        assert!(frames
            .iter()
            .all(|f| f.ids.len() == 32 && f.text.len() < 128));
        let tx = store.database.begin_read().unwrap();
        assert_eq!(tx.open_table(RECORDS).unwrap().iter().unwrap().count(), 300);
        assert_eq!(tx.open_table(PENDING).unwrap().iter().unwrap().count(), 44);
        assert_eq!(
            tx.open_table(ACTIVE_FRAMES)
                .unwrap()
                .iter()
                .unwrap()
                .count(),
            8
        );
        drop(tx);
        let snapshot = store.attention_snapshot(&scope(), None, 100).unwrap();
        assert_eq!(snapshot.records.len(), 100);
        let cursor = snapshot.next_cursor.unwrap();
        assert_eq!(
            store
                .attention_snapshot(&scope(), Some(&cursor), 100)
                .unwrap()
                .records
                .len(),
            100
        );
        admit(&store, "new");
        assert!(store
            .attention_snapshot(&scope(), Some(&cursor), 100)
            .is_err());
    }

    #[test]
    fn ordinary_explicit_question_is_admitted_and_deduplicated() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        for _ in 0..2 {
            let tx = store.database.begin_write().unwrap();
            let mut question = source("ordinary-lookup");
            question.category = AttentionCategory::Question;
            question.cause_id = "explicit-ask".into();
            RuntimeStore::admit_attention_in(&tx, question, 1).unwrap();
            tx.commit().unwrap();
        }
        let records = store
            .attention_snapshot(&scope(), None, 10)
            .unwrap()
            .records;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].category, AttentionCategory::Question);
        assert_eq!(
            store.claim_attention_frame(200).unwrap().unwrap().ids,
            vec![records[0].id.clone()]
        );
    }

    #[test]
    fn urgent_precedes_questions_and_forged_publication_cannot_ack() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let tx = store.database.begin_write().unwrap();
        let mut question = source("question");
        question.category = AttentionCategory::Question;
        RuntimeStore::admit_attention_in(&tx, question, 1).unwrap();
        tx.commit().unwrap();
        let failure = admit(&store, "failed");
        let frame = store.claim_attention_frame(200).unwrap().unwrap();
        assert_eq!(frame.ids, vec![failure.id]);
        let mut event = publication(&frame);
        event.metadata.generation = 1;
        assert!(store.admit_attention_publication(&mut event, 300).is_err());
        event.metadata.generation = 0;
        event.metadata.conversation_id = "other".into();
        assert!(store.admit_attention_publication(&mut event, 300).is_err());
        assert!(store.pending_history().unwrap().is_empty());
        assert!(store
            .attention_snapshot(&scope(), None, 10)
            .unwrap()
            .records
            .iter()
            .all(|r| r.delivered_at_ms.is_none()));
    }

    #[test]
    fn publication_enrichment_preserves_exact_coalesced_ids_in_canonical_history() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        admit(&store, "first");
        admit(&store, "second");
        let frame = store.claim_attention_frame(200).unwrap().unwrap();
        assert_eq!(frame.ids.len(), 2);
        let mut event = publication(&frame);
        event.metadata.attention = Some(AttentionFrameMetadata {
            scope: scope(),
            ids: vec!["forged".into()],
        });
        assert!(store.admit_attention_publication(&mut event, 300).is_err());
        assert!(store.pending_history().unwrap().is_empty());
        event.metadata.attention = None;
        store.admit_attention_publication(&mut event, 300).unwrap();
        let expected = AttentionFrameMetadata {
            scope: frame.scope,
            ids: frame.ids,
        };
        assert_eq!(event.metadata.attention, Some(expected.clone()));
        let initial = store.attention_snapshot(&scope(), None, 10).unwrap();
        store.admit_attention_publication(&mut event, 400).unwrap();
        assert_eq!(
            initial,
            store.attention_snapshot(&scope(), None, 10).unwrap()
        );
        let projections = store.pending_history().unwrap();
        assert_eq!(projections.len(), 1);
        assert_eq!(projections[0].attention, Some(expected.clone()));
        let path = dir.path().join("history.redb");
        {
            let history = crate::history_store::HistoryStore::open(&path).unwrap();
            assert!(history.apply(&projections[0]).unwrap());
            assert!(!history.apply(&projections[0]).unwrap());
        }
        let history = crate::history_store::HistoryStore::open(&path).unwrap();
        let entries = history.activity_between(0, 500, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].attention, Some(expected));
        assert_eq!(entries[0].event_id, event.metadata.message_id);
        assert_eq!(entries[0].text, projections[0].text);
        assert!(initial
            .records
            .iter()
            .all(|r| r.delivered_at_ms == Some(300)
                && r.displayed_at_ms.is_none()
                && r.acknowledged_at_ms.is_none()));
    }

    #[test]
    fn campaign_source_rejects_stale_revision_and_generation() {
        use super::super::{
            admission::Admission,
            campaign_ledger::{Envelope, Pool, Units},
        };
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: "r".into(),
                title: "r".into(),
                objective: "r".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: "c".into(),
                research_id: research.id,
                title: "c".into(),
                objective: "c".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        store
            .host_authorize_campaign_envelope(
                "grant",
                &campaign.id,
                Envelope {
                    work: Units {
                        tokens: 10,
                        cost_micro_usd: 10,
                    },
                    verification: Units::default(),
                    max_active_inferences: 1,
                },
            )
            .unwrap();
        store
            .admit_campaign_work(Admission {
                work_id: "work".into(),
                campaign_id: campaign.id.clone(),
                objective: "test".into(),
                instruction_revision: 1,
                generation: 1,
                pool: Pool::Work,
                upper_bound: Units {
                    tokens: 1,
                    cost_micro_usd: 1,
                },
            })
            .unwrap();
        for (generation, revision) in [(2, 1), (1, 2)] {
            let tx = store.database.begin_write().unwrap();
            let mut source = source("work");
            source.campaign_id = Some(campaign.id.clone());
            source.scope = TodoScope::Campaign {
                campaign_id: campaign.id.clone(),
            };
            source.generation = generation;
            source.instruction_revision = revision;
            assert!(RuntimeStore::admit_attention_in(&tx, source, 1).is_err());
        }
        assert!(store
            .attention_snapshot(
                &TodoScope::Campaign {
                    campaign_id: campaign.id.clone()
                },
                None,
                10
            )
            .unwrap()
            .records
            .is_empty());
        // A campaign is independent work even when its registry correlation looks
        // exactly like a foreground-owned retrieval.
        let mut registry = crate::Registry::default();
        registry.foreground_id = Some(tachyon_api::FOREGROUND_ID.into());
        registry.tasks.insert(
            tachyon_api::FOREGROUND_ID.into(),
            crate::tests::task(tachyon_api::FOREGROUND_ID, tachyon_api::AgentState::Running),
        );
        let mut worker = crate::tests::task("worker", tachyon_api::AgentState::Running);
        worker.info.owner = "background".into();
        worker.info.origin_turn_id = Some("conversation:weather:7".into());
        worker.info.logical_task_id = Some("work".into());
        worker.generation = 1;
        worker.assignment = 1;
        registry.works.insert(
            "work".into(),
            crate::WorkRecord {
                observed_calls: Default::default(),
                partial_evidence: Default::default(),
                request: serde_json::from_value(serde_json::json!({
                    "work_id":"work", "objective":"training", "generation":1,
                    "assignment":1, "deadline_ms":100, "lifetime_class":"long"
                }))
                .unwrap(),
                fingerprint: "test".into(),
                worker_id: "worker".into(),
                info: worker.info.clone(),
                review: None,
                terminal_result: None,
                subs: vec![],
            },
        );
        registry.tasks.insert("worker".into(), worker);
        let mut result: tachyon_api::WorkResult = serde_json::from_value(serde_json::json!({
            "work_id":"work", "objective":"training", "generation":1,
            "assignment":1, "instruction_revision":2,
            "outcome":"failed", "message":"training crashed"
        }))
        .unwrap();
        store.record_terminal_attention(&result, &registry).unwrap();
        assert!(store.claim_attention_frame(100).unwrap().is_none());
        result.instruction_revision = Some(1);
        store.record_terminal_attention(&result, &registry).unwrap();
        store.record_terminal_attention(&result, &registry).unwrap();
        let records = store
            .attention_snapshot(
                &TodoScope::Campaign {
                    campaign_id: campaign.id,
                },
                None,
                10,
            )
            .unwrap()
            .records;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].category, AttentionCategory::WorkFailed);
        assert_eq!(
            store.claim_attention_frame(200).unwrap().unwrap().ids,
            vec![records[0].id.clone()]
        );
    }

    #[test]
    fn local_socket_endpoints_are_scoped_and_internal_dispatch_denies_access() {
        use std::{
            io::{BufRead, BufReader, Write},
            os::unix::net::UnixStream,
            sync::{Arc, Mutex},
        };
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let record = admit(&store, "work");
        let registry = Arc::new(Mutex::new(crate::Registry {
            runtime_store: Some(store),
            ..Default::default()
        }));
        let request = ApiRequest::AttentionList {
            scope: scope(),
            after: None,
            limit: 10,
        };
        assert!(matches!(
            crate::dispatch(&request, &registry),
            ApiResponse::Error { .. }
        ));
        let (mut client, server) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let serving =
            std::thread::spawn(move || crate::handle_connection(server, registry).unwrap());
        let mut reader = BufReader::new(client.try_clone().unwrap());
        for request in [
            request,
            ApiRequest::AttentionAcknowledge {
                scope: scope(),
                id: record.id.clone(),
                phase: AttentionAcknowledgement::Acknowledged,
            },
        ] {
            writeln!(client, "{}", serde_json::to_string(&request).unwrap()).unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let response: ApiResponse = serde_json::from_str(&line).unwrap();
            match response {
                ApiResponse::AttentionList { snapshot } => {
                    assert_eq!(snapshot.records, vec![record.clone()])
                }
                ApiResponse::AttentionAcknowledged { attention } => {
                    assert!(attention.acknowledged_at_ms.is_some())
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        drop(reader);
        drop(client);
        serving.join().unwrap();
    }
}
fn err(e: impl std::fmt::Display) -> String {
    format!("attention: {e}")
}
fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(v).map_err(err)
}
fn decode<T: serde::de::DeserializeOwned>(v: &[u8]) -> Result<T, String> {
    serde_json::from_slice(v).map_err(err)
}
fn priority(category: AttentionCategory) -> u8 {
    u8::from(category.severity() != AttentionSeverity::Urgent)
}
fn scope_key(scope: &TodoScope) -> Result<String, String> {
    let id = match scope {
        TodoScope::Conversation { id } => id,
        TodoScope::Campaign { campaign_id } => campaign_id,
        TodoScope::Work { work_id } => work_id,
    };
    if id.trim().is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
        return Err(err("invalid scope"));
    }
    serde_json::to_string(scope).map_err(err)
}

/// Construct only after validating the source in its mutation transaction.
/// This type is intentionally not deserializable or exposed to worker tools.
pub(crate) struct AttentionSource {
    pub scope: TodoScope,
    pub work_id: Option<String>,
    pub campaign_id: Option<String>,
    pub generation: u64,
    pub instruction_revision: u64,
    pub category: AttentionCategory,
    pub cause_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct AttentionFrame {
    pub command_id: String,
    pub scope: TodoScope,
    pub ids: Vec<String>,
    pub text: String,
    pub attempted_at_ms: u64,
    pub delivered: bool,
}

fn save(tx: &WriteTransaction, record: &Attention, now: u64) -> Result<(), String> {
    tx.open_table(RECORDS)
        .map_err(err)?
        .insert(record.id.as_str(), encode(record)?.as_slice())
        .map_err(err)?;
    feed::append(
        tx,
        OperationalEvent {
            schema_version: 1,
            watermark: OperationalWatermark {
                instance_id: String::new(),
                sequence: 0,
            },
            scope: record.scope.clone(),
            scope_revision: 0,
            occurred_at_ms: now,
            change: OperationalChange::AttentionChanged {
                attention: record.clone(),
            },
        },
    )?;
    Ok(())
}

impl RuntimeStore {
    /// Called only by the daemon's exact assignment-fenced terminal path.
    /// Intermediate candidates/retries and generic error logs never reach here.
    pub(crate) fn record_terminal_attention(
        &self,
        result: &tachyon_api::WorkResult,
        registry: &crate::Registry,
    ) -> Result<(), String> {
        let category = match result.outcome {
            tachyon_api::WorkOutcome::Failed { .. } => AttentionCategory::WorkFailed,
            tachyon_api::WorkOutcome::TimedOut { .. } => AttentionCategory::WorkTimedOut,
            _ => return Ok(()),
        };
        let tx = self.database.begin_write().map_err(err)?;
        // Ordinary delegated work has no campaign admission. Campaign work must
        // additionally match its durable generation and current instruction fence.
        let admitted = tx
            .open_table(super::admission::WORK)
            .map_err(err)?
            .get(result.work_id.as_str())
            .map_err(err)?
            .is_some();
        let (scope, campaign_id, revision) = if admitted {
            let work = Self::admitted_work_in(&tx, &result.work_id)?;
            let revision = Self::latest_instruction_revision_in(&tx, &work.admission)?;
            if work.admission.generation != result.generation
                || result.instruction_revision != Some(revision)
            {
                return Ok(());
            }
            (
                TodoScope::Campaign {
                    campaign_id: work.admission.campaign_id.clone(),
                },
                Some(work.admission.campaign_id),
                revision,
            )
        } else {
            // Inspect host registry metadata, never worker-supplied correlation or
            // error prose. The retained WorkRecord is the outcome route even when
            // the foreground has not yet attached its result subscriber.
            let foreground_owned = registry.works.get(&result.work_id).is_some_and(|work| {
                registry.tasks.get(&work.worker_id).is_some_and(|task| {
                    matches!(task.info.owner.as_str(), "foreground" | "background")
                        && task
                            .info
                            .origin_turn_id
                            .as_deref()
                            .is_some_and(|turn| !turn.trim().is_empty())
                        && task.info.logical_task_id.as_deref() == Some(result.work_id.as_str())
                        && task.generation == result.generation
                        && task.assignment == result.assignment
                }) && registry.foreground_id.as_deref() == Some(tachyon_api::FOREGROUND_ID)
                    && registry
                        .tasks
                        .get(tachyon_api::FOREGROUND_ID)
                        .is_some_and(|task| task.info.state == tachyon_api::AgentState::Running)
            });
            if foreground_owned {
                return Ok(());
            }
            (
                TodoScope::Conversation {
                    id: tachyon_api::FOREGROUND_ID.into(),
                },
                None,
                0,
            )
        };
        Self::admit_attention_in(
            &tx,
            AttentionSource {
                scope,
                work_id: Some(result.work_id.clone()),
                campaign_id,
                generation: result.generation,
                instruction_revision: revision,
                category,
                cause_id: format!("terminal:{}", result.assignment),
            },
            crate::unix_now_ms(),
        )?;
        tx.commit().map_err(err)
    }

    pub(crate) fn admit_attention_in(
        tx: &WriteTransaction,
        source: AttentionSource,
        now: u64,
    ) -> Result<Attention, String> {
        let scope = scope_key(&source.scope)?;
        if let Some(campaign) = &source.campaign_id {
            Self::campaign_status_in(tx, campaign)?;
            if source.scope
                != (TodoScope::Campaign {
                    campaign_id: campaign.clone(),
                })
            {
                return Err(err("campaign scope mismatch"));
            }
            if let Some(id) = &source.work_id {
                let work = Self::admitted_work_in(tx, id)?;
                if work.admission.campaign_id != *campaign
                    || work.admission.generation != source.generation
                    || Self::latest_instruction_revision_in(tx, &work.admission)?
                        != source.instruction_revision
                {
                    return Err(err("stale attention source"));
                }
            }
        }
        let key = serde_json::to_string(&(
            &source.scope,
            &source.work_id,
            &source.campaign_id,
            source.generation,
            source.instruction_revision,
            source.category,
            &source.cause_id,
        ))
        .map_err(err)?;
        if key.len() > 4096 || source.cause_id.trim().is_empty() {
            return Err(err("invalid source identity"));
        }
        if let Some(id) = tx
            .open_table(CAUSES)
            .map_err(err)?
            .get(key.as_str())
            .map_err(err)?
        {
            let table = tx.open_table(RECORDS).map_err(err)?;
            return decode(
                table
                    .get(id.value())
                    .map_err(err)?
                    .ok_or_else(|| err("missing record"))?
                    .value(),
            );
        }
        let id = format!("attention-{}", uuid::Uuid::new_v4());
        let record = Attention {
            command_id: format!("attention-command-{id}"),
            id,
            cause_id: source.cause_id,
            scope: source.scope,
            work_id: source.work_id,
            campaign_id: source.campaign_id,
            generation: source.generation,
            instruction_revision: source.instruction_revision,
            category: source.category,
            severity: source.category.severity(),
            accepted_at_ms: now,
            delivered_at_ms: None,
            displayed_at_ms: None,
            acknowledged_at_ms: None,
        };
        save(tx, &record, now)?;
        tx.open_table(CAUSES)
            .map_err(err)?
            .insert(key.as_str(), record.id.as_str())
            .map_err(err)?;
        tx.open_table(SCOPED)
            .map_err(err)?
            .insert((scope.as_str(), record.id.as_str()), ())
            .map_err(err)?;
        tx.open_table(PENDING)
            .map_err(err)?
            .insert((priority(record.category), record.id.as_str()), ())
            .map_err(err)?;
        Ok(record)
    }

    pub(crate) fn attention_snapshot(
        &self,
        scope: &TodoScope,
        after: Option<&str>,
        limit: usize,
    ) -> Result<AttentionSnapshot, String> {
        let key = scope_key(scope)?;
        if !(1..=100).contains(&limit) || after.is_some_and(|s| s.len() > 1024) {
            return Err(err("invalid list bounds"));
        }
        let tx = self.database.begin_read().map_err(err)?;
        let watermark = feed::watermark(&tx.open_table(feed::METADATA).map_err(err)?)?;
        let after = if let Some(cursor) = after {
            let (mark, bound_scope, id): (OperationalWatermark, TodoScope, String) =
                serde_json::from_str(cursor).map_err(err)?;
            if mark != watermark || &bound_scope != scope {
                return Err(err("stale snapshot cursor; restart list"));
            }
            Some(id)
        } else {
            None
        };
        let table = tx.open_table(RECORDS).map_err(err)?;
        let mut records = Vec::new();
        use std::ops::Bound::{Excluded, Included};
        let index = tx.open_table(SCOPED).map_err(err)?;
        let lower = after.as_deref().map_or(Included((key.as_str(), "")), |id| {
            Excluded((key.as_str(), id))
        });
        for row in index
            .range((lower, Included((key.as_str(), "\u{7f}"))))
            .map_err(err)?
            .take(limit + 1)
        {
            let (id, _) = row.map_err(err)?;
            let bytes = table
                .get(id.value().1)
                .map_err(err)?
                .ok_or_else(|| err("missing scoped record"))?;
            let record: Attention = decode(bytes.value())?;
            if &record.scope == scope {
                records.push(record);
            }
            if records.len() > limit {
                break;
            }
        }
        let next_cursor = if records.len() > limit {
            records.pop();
            Some(
                serde_json::to_string(&(&watermark, scope, &records.last().unwrap().id))
                    .map_err(err)?,
            )
        } else {
            None
        };
        Ok(AttentionSnapshot {
            records,
            next_cursor,
            watermark,
        })
    }

    pub(crate) fn acknowledge_attention(
        &self,
        scope: &TodoScope,
        id: &str,
        phase: AttentionAcknowledgement,
        now: u64,
    ) -> Result<Attention, String> {
        scope_key(scope)?;
        if id.len() > 256 {
            return Err(err("invalid attention ID"));
        }
        let tx = self.database.begin_write().map_err(err)?;
        let mut record: Attention = {
            let table = tx.open_table(RECORDS).map_err(err)?;
            let value = table
                .get(id)
                .map_err(err)?
                .ok_or_else(|| err("unknown attention"))?;
            decode(value.value())?
        };
        if &record.scope != scope {
            return Err(err("scope mismatch"));
        }
        let target = match phase {
            AttentionAcknowledgement::Displayed => {
                if record.delivered_at_ms.is_none() {
                    return Err(err("not delivered"));
                }
                &mut record.displayed_at_ms
            }
            AttentionAcknowledgement::Acknowledged => &mut record.acknowledged_at_ms,
        };
        if target.is_some() {
            return Ok(record);
        }
        *target = Some(now);
        save(&tx, &record, now)?;
        tx.commit().map_err(err)?;
        Ok(record)
    }

    /// At most one frame per poll and 32 underlying records per frame. Existing
    /// frames retry unchanged; absence of a UI never discards underlying records.
    pub(crate) fn claim_attention_frame(&self, now: u64) -> Result<Option<AttentionFrame>, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let mut frame = None;
        let mut outstanding = 0;
        let frames = tx.open_table(FRAMES).map_err(err)?;
        for row in tx
            .open_table(ACTIVE_FRAMES)
            .map_err(err)?
            .iter()
            .map_err(err)?
        {
            let (id, _) = row.map_err(err)?;
            let bytes = frames
                .get(id.value())
                .map_err(err)?
                .ok_or_else(|| err("missing active frame"))?;
            let candidate: AttentionFrame = decode(bytes.value())?;
            if !candidate.delivered {
                outstanding += 1;
                if candidate.attempted_at_ms.saturating_add(5000) <= now {
                    frame = Some(candidate);
                    break;
                }
            }
        }
        drop(frames);
        if frame.is_none() && outstanding < 8 {
            let mut records = Vec::<Attention>::new();
            let table = tx.open_table(RECORDS).map_err(err)?;
            for row in tx
                .open_table(PENDING)
                .map_err(err)?
                .iter()
                .map_err(err)?
                .take(128)
            {
                let (key, _) = row.map_err(err)?;
                let record: Attention = decode(
                    table
                        .get(key.value().1)
                        .map_err(err)?
                        .ok_or_else(|| err("missing pending record"))?
                        .value(),
                )?;
                if records
                    .first()
                    .is_none_or(|r| r.scope == record.scope && r.category == record.category)
                {
                    records.push(record);
                }
                if records.len() == 32 {
                    break;
                }
            }
            if let Some(first) = records.first() {
                let label = match first.category {
                    AttentionCategory::WorkFailed => "Work failed",
                    AttentionCategory::WorkTimedOut => "Work timed out",
                    AttentionCategory::BudgetBlocked => "Work is blocked by its budget",
                    AttentionCategory::Question => "Work needs your input",
                };
                frame = Some(AttentionFrame {
                    command_id: first.command_id.clone(),
                    scope: first.scope.clone(),
                    ids: records.iter().map(|r| r.id.clone()).collect(),
                    text: format!(
                        "{label} ({} item{}). Review attention for details.",
                        records.len(),
                        if records.len() == 1 { "" } else { "s" }
                    ),
                    attempted_at_ms: now,
                    delivered: false,
                });
                for record in records {
                    tx.open_table(PENDING)
                        .map_err(err)?
                        .remove((priority(record.category), record.id.as_str()))
                        .map_err(err)?;
                }
            }
        }
        if let Some(frame) = &mut frame {
            frame.attempted_at_ms = now;
            tx.open_table(FRAMES)
                .map_err(err)?
                .insert(frame.command_id.as_str(), encode(frame)?.as_slice())
                .map_err(err)?;
            tx.open_table(ACTIVE_FRAMES)
                .map_err(err)?
                .insert(frame.command_id.as_str(), ())
                .map_err(err)?;
        }
        tx.commit().map_err(err)?;
        Ok(frame)
    }

    /// Publication is durably admitted to history in this same transaction.
    /// Delivered means host admission, not that a UI displayed it.
    pub(crate) fn admit_attention_publication(
        &self,
        envelope: &mut tachyon_api::InteractionEventEnvelope,
        now: u64,
    ) -> Result<bool, String> {
        let Some(command) = envelope.metadata.causation_id.as_deref() else {
            return Ok(false);
        };
        if !command.starts_with("attention-command-") {
            return Ok(false);
        }
        let tx = self.database.begin_write().map_err(err)?;
        let mut frame: AttentionFrame = {
            let table = tx.open_table(FRAMES).map_err(err)?;
            let value = table
                .get(command)
                .map_err(err)?
                .ok_or_else(|| err("unknown frame"))?;
            decode(value.value())?
        };
        let tachyon_api::InteractionEvent::UserVisibleNotificationPublished { text } =
            &envelope.event
        else {
            return Err(err("invalid publication phase"));
        };
        if text != &frame.text
            || envelope.metadata.message_id != format!("{command}:published")
            || envelope.metadata.correlation_id != command
            || envelope.metadata.conversation_id != tachyon_api::FOREGROUND_ID
            || envelope.metadata.turn_id.is_some()
            || envelope.metadata.generation != 0
            || envelope.metadata.protocol_version != tachyon_api::INTERACTION_PROTOCOL_VERSION
        {
            return Err(err("publication identity mismatch"));
        }
        let attention = AttentionFrameMetadata {
            scope: frame.scope.clone(),
            ids: frame.ids.clone(),
        };
        if envelope
            .metadata
            .attention
            .as_ref()
            .is_some_and(|supplied| supplied != &attention)
        {
            return Err(err("publication membership mismatch"));
        }
        // Only the persisted host frame is authority, including legacy publications.
        envelope.metadata.attention = Some(attention.clone());
        if frame.delivered {
            return Ok(true);
        }
        let projection = super::HistoryProjection {
            attention: Some(attention),
            schema_version: 1,
            event_id: envelope.metadata.message_id.clone(),
            kind: tachyon_api::HistoryKind::Conversation,
            conversation_id: envelope.metadata.conversation_id.clone(),
            turn_id: None,
            occurred_at_ms: now,
            role: tachyon_api::HistoryRole::Notification,
            text: text.clone(),
            task_id: None,
            task_state: None,
        };
        tx.open_table(super::HISTORY_OUTBOX)
            .map_err(err)?
            .insert(
                projection.event_id.as_str(),
                encode(&projection)?.as_slice(),
            )
            .map_err(err)?;
        for id in &frame.ids {
            let mut record: Attention = {
                let table = tx.open_table(RECORDS).map_err(err)?;
                let value = table
                    .get(id.as_str())
                    .map_err(err)?
                    .ok_or_else(|| err("missing frame record"))?;
                decode(value.value())?
            };
            record.delivered_at_ms = Some(now);
            save(&tx, &record, now)?;
        }
        frame.delivered = true;
        tx.open_table(ACTIVE_FRAMES)
            .map_err(err)?
            .remove(command)
            .map_err(err)?;
        tx.open_table(FRAMES)
            .map_err(err)?
            .insert(command, encode(&frame)?.as_slice())
            .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(true)
    }
}
