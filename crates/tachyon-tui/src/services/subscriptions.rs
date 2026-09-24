//! Owned live readers. Slow consumers backpressure sockets, not event loss.
use super::super::{
    attention, decode_interaction_event, is_structured_legacy_marker, now_seconds, session_archive,
    TuiEvent, FOREGROUND_ID,
};
use super::event_buffer;
use std::{
    collections::HashMap,
    io::{self, BufReader},
    net::Shutdown,
    os::unix::net::UnixStream,
    sync::{Arc, Mutex, Weak},
    thread::JoinHandle,
    time::Duration,
};
use tachyon_api::{
    transport::{read_response, write_request},
    types::{AgentEvent, ApiRequest, ApiResponse, EventEnvelope},
};
use tachyon_client::Client;

const EVENT_CAPACITY: usize = 256;
const PAYLOAD_BUDGET: usize = 4 * 1024 * 1024;

struct Message {
    source: String,
    generation: u64,
    event: TuiEvent,
}

#[derive(Clone)]
struct Publisher {
    source: String,
    generation: u64,
    out: event_buffer::Sender<Message>,
}

impl Publisher {
    fn send(&self, event: TuiEvent) -> Result<(), String> {
        // Count encoded payload without allocating a second serialization buffer.
        struct Count(usize);
        impl io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0 = self.0.saturating_add(bytes.len());
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut bytes = Count(std::mem::size_of::<Message>() + self.source.len());
        match &event {
            TuiEvent::Manager(frame) => {
                serde_json::to_writer(&mut bytes, frame).map_err(|e| e.to_string())?;
            }
            TuiEvent::Interaction { agent_id, envelope } => {
                bytes.0 += agent_id.len();
                serde_json::to_writer(&mut bytes, envelope).map_err(|e| e.to_string())?;
            }
            TuiEvent::Structured { agent_id, envelope } => {
                bytes.0 += agent_id.len();
                serde_json::to_writer(&mut bytes, envelope).map_err(|e| e.to_string())?;
            }
            TuiEvent::Recovered(entry) => {
                serde_json::to_writer(&mut bytes, entry).map_err(|e| e.to_string())?;
            }
            TuiEvent::Line { agent_id, data, .. } => bytes.0 += agent_id.len() + data.len(),
            TuiEvent::Ended { agent_id, summary } => bytes.0 += agent_id.len() + summary.len(),
            TuiEvent::ChatResult { error } => bytes.0 += error.as_ref().map_or(0, String::len),
            _ => unreachable!("not a subscription event"),
        }
        self.out
            .send(
                Message {
                    source: self.source.clone(),
                    generation: self.generation,
                    event,
                },
                bytes.0,
            )
            .map_err(|()| "subscription cancelled".into())
    }
}

struct Sockets(Mutex<Option<Vec<Weak<UnixStream>>>>);

impl Sockets {
    fn register(&self, socket: UnixStream) -> io::Result<Arc<UnixStream>> {
        let mut sockets = self.0.lock().unwrap();
        if let Some(sockets) = sockets.as_mut() {
            let socket = Arc::new(socket);
            sockets.push(Arc::downgrade(&socket));
            Ok(socket)
        } else {
            let _ = socket.shutdown(Shutdown::Both);
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "subscription cancelled",
            ))
        }
    }

    fn stop(&self) {
        if let Some(sockets) = self.0.lock().unwrap().take() {
            for socket in sockets {
                if let Some(socket) = socket.upgrade() {
                    let _ = socket.shutdown(Shutdown::Both);
                }
            }
        }
    }
}

struct Worker {
    generation: u64,
    out: event_buffer::Sender<Message>,
    sockets: Arc<Sockets>,
    thread: JoinHandle<()>,
}

impl Worker {
    fn stop(&self) {
        self.out.cancel();
        self.sockets.stop();
    }
}

pub(in crate::app) struct Subscriptions {
    out: event_buffer::Sender<Message>,
    incoming: event_buffer::Receiver<Message>,
    active: HashMap<String, Worker>,
    retired: Vec<Worker>,
    generation: u64,
}

impl Default for Subscriptions {
    fn default() -> Self {
        let (out, incoming) = event_buffer::channel(EVENT_CAPACITY, PAYLOAD_BUDGET);
        Self {
            out,
            incoming,
            active: HashMap::new(),
            retired: Vec::new(),
            generation: 0,
        }
    }
}

impl Subscriptions {
    #[cfg(test)]
    pub(in crate::app) fn attach_fixture(&mut self, socket: UnixStream) -> io::Result<()> {
        self.start_with(
            super::interaction::SOURCE.into(),
            move || Ok(socket),
            Vec::new(),
            false,
        )
    }

    pub(in crate::app) fn contains(&self, source: &str) -> bool {
        self.active.contains_key(source)
    }

    pub(in crate::app) fn start(
        &mut self,
        source: String,
        pending: Vec<(String, u64)>,
    ) -> io::Result<()> {
        self.start_with(
            source,
            move || UnixStream::connect(tachyon_util::daemon::socket_path()),
            pending,
            true,
        )
    }

    fn start_with(
        &mut self,
        source: String,
        connect: impl FnOnce() -> io::Result<UnixStream> + Send + 'static,
        pending: Vec<(String, u64)>,
        recover_attention: bool,
    ) -> io::Result<()> {
        self.end(&source);
        self.generation = self
            .generation
            .checked_add(1)
            .expect("subscription generation exhausted");
        let publisher = Publisher {
            source: source.clone(),
            generation: self.generation,
            out: self.out.fork(),
        };
        let sockets = Arc::new(Sockets(Mutex::new(Some(Vec::new()))));
        let worker_sockets = sockets.clone();
        let out = publisher.out.clone();
        let thread = std::thread::Builder::new()
            .name("tui-subscription".into())
            .spawn(move || {
                let result = (|| -> Result<(), String> {
                    let mut socket = connect().map_err(|e| e.to_string())?;
                    let _shutdown = worker_sockets
                        .register(socket.try_clone().map_err(|e| e.to_string())?)
                        .map_err(|e| e.to_string())?;
                    socket
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .map_err(|e| e.to_string())?;
                    write_request(
                        &mut socket,
                        &if publisher.source == super::interaction::SOURCE {
                            ApiRequest::InteractionAttach { after: None }
                        } else {
                            ApiRequest::AgentSubscribe {
                                id: publisher.source.clone(),
                            }
                        },
                    )
                    .map_err(|e| e.to_string())?;
                    // Subscribe before querying history, so live finals follow recovery.
                    if !pending.is_empty() {
                        if let Err(error) = recover(&publisher, &worker_sockets, Some(&pending)) {
                            publisher.send(TuiEvent::ChatResult {
                                error: Some(format!("History recovery: {error}")),
                            })?;
                        }
                    }
                    std::thread::scope(|scope| {
                        if recover_attention && publisher.source == super::interaction::SOURCE {
                            scope.spawn(|| {
                                if let Err(error) = recover(&publisher, &worker_sockets, None) {
                                    let _ = publisher.send(TuiEvent::ChatResult {
                                        error: Some(format!("Attention recovery: {error}")),
                                    });
                                }
                            });
                        }
                        let mut reader = BufReader::new(socket);
                        let result = if publisher.source == super::interaction::SOURCE {
                            super::interaction::read(&mut reader, |event| publisher.send(event))
                        } else {
                            read_events(&mut reader, &publisher)
                        };
                        let _ = publisher.send(TuiEvent::Ended {
                            agent_id: publisher.source.clone(),
                            summary: result.err().unwrap_or_else(|| "stream ended".into()),
                        });
                        // Wake recovery before scope joins it, including a full queue.
                        publisher.out.cancel();
                        worker_sockets.stop();
                    });
                    Ok(())
                })();
                if let Err(summary) = result {
                    let _ = publisher.send(TuiEvent::Ended {
                        agent_id: publisher.source.clone(),
                        summary,
                    });
                }
                publisher.out.cancel();
                worker_sockets.stop();
            })?;
        self.active.insert(
            source,
            Worker {
                generation: self.generation,
                out,
                sockets,
                thread,
            },
        );
        Ok(())
    }

    /// At most one dequeue, including stale events, per scheduler budget tick.
    pub(in crate::app) fn poll(&self) -> Option<Option<TuiEvent>> {
        let message = self.incoming.try_recv()?;
        Some(
            self.active
                .get(&message.source)
                .filter(|worker| worker.generation == message.generation)
                .map(|_| message.event),
        )
    }

    pub(in crate::app) fn end(&mut self, source: &str) {
        if let Some(worker) = self.active.remove(source) {
            worker.stop();
            self.retired.push(worker);
        }
    }

    pub(in crate::app) fn reap(&mut self) {
        let mut index = 0;
        while index < self.retired.len() {
            if self.retired[index].thread.is_finished() {
                let _ = self.retired.swap_remove(index).thread.join();
            } else {
                index += 1;
            }
        }
    }

    pub(in crate::app) fn stop(&mut self) {
        for (_, worker) in self.active.drain() {
            worker.stop();
            self.retired.push(worker);
        }
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        self.stop();
        for worker in self.retired.drain(..) {
            let _ = worker.thread.join();
        }
    }
}

fn recover(
    publisher: &Publisher,
    sockets: &Sockets,
    pending: Option<&[(String, u64)]>,
) -> Result<(), String> {
    let mut client = Client::connect().map_err(|e| e.to_string())?;
    let _shutdown = sockets
        .register(client.shutdown_handle().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let publish = |entry| publisher.send(TuiEvent::Recovered(entry));
    if let Some(pending) = pending {
        session_archive::recover_continuations(pending, &mut client, publish)
    } else {
        attention::recover(&mut client, publish)
    }
}

fn read_events(reader: &mut BufReader<UnixStream>, publisher: &Publisher) -> Result<(), String> {
    loop {
        match read_response(reader).map_err(|e| e.to_string())? {
            ApiResponse::Event { stream, data } => {
                let agent_id = publisher.source.clone();
                // Worker assertions and legacy foreground copies are not response authority.
                if decode_interaction_event(&data).is_some() {
                    continue;
                }
                let event = if let Ok(envelope) = serde_json::from_str::<EventEnvelope>(&data)
                    .or_else(|_| {
                        serde_json::from_str::<AgentEvent>(&data).map(|kind| EventEnvelope {
                            event_id: 0,
                            session_id: agent_id.clone(),
                            conversation_id: None,
                            turn_id: None,
                            task_id: None,
                            parent_task_id: None,
                            tool_call_id: None,
                            actor: tachyon_api::types::Actor::System,
                            sequence: 0,
                            occurred_at_ms: now_seconds(),
                            kind,
                        })
                    }) {
                    if agent_id == FOREGROUND_ID
                        && matches!(
                            envelope.kind,
                            AgentEvent::Reply { .. }
                                | AgentEvent::ReplyDelta { .. }
                                | AgentEvent::Status { .. }
                                | AgentEvent::Error { .. }
                        )
                    {
                        continue;
                    }
                    drop(data);
                    TuiEvent::Structured { agent_id, envelope }
                } else if is_structured_legacy_marker(&data) {
                    continue;
                } else {
                    if agent_id == FOREGROUND_ID {
                        continue;
                    }
                    TuiEvent::Line {
                        agent_id,
                        stream,
                        data,
                    }
                };
                publisher.send(event)?;
            }
            ApiResponse::Error { message, .. } => return Err(message),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{apply_correlated_agent_event, ItemKind, Thread};
    use std::{io::Write, time::Instant};
    use tachyon_api::{transport::write_response, types::EventStream};

    #[test]
    fn foreground_raw_overlap_is_filtered_but_host_scoped_telemetry_survives() {
        let mut subscriptions = Subscriptions::default();
        let (reader, mut writer) = UnixStream::pair().unwrap();
        subscriptions
            .start_with(FOREGROUND_ID.into(), move || Ok(reader), Vec::new(), false)
            .unwrap();
        let mut request = BufReader::new(writer.try_clone().unwrap());
        assert!(matches!(
            tachyon_api::transport::read_request(&mut request).unwrap(),
            ApiRequest::AgentSubscribe { .. }
        ));
        drop(request);
        let mut metadata =
            tachyon_api::InteractionMetadata::new("raw-copy", "request", FOREGROUND_ID, 1);
        metadata.turn_id = Some("host:1".into());
        write_response(
            &mut writer,
            &ApiResponse::Event {
                stream: EventStream::Stdout,
                data: serde_json::to_string(&tachyon_api::InteractionEventEnvelope {
                    metadata,
                    event: tachyon_api::InteractionEvent::ConversationFinished {
                        text: "duplicate".into(),
                    },
                })
                .unwrap(),
            },
        )
        .unwrap();
        write(&mut writer, &reply("overlapping raw reply".into(), true));
        write(
            &mut writer,
            &AgentEvent::Error {
                turn: Some(1),
                message: "raw error must not terminate manager reply".into(),
            },
        );
        let envelope = EventEnvelope {
            event_id: 1,
            session_id: "host".into(),
            conversation_id: Some(FOREGROUND_ID.into()),
            turn_id: Some("host:1".into()),
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::Actor::Foreground,
            sequence: 1,
            occurred_at_ms: 1,
            kind: AgentEvent::Timing {
                turn: 1,
                stage: "first_visible".into(),
                elapsed_ms: 42,
            },
        };
        write_response(
            &mut writer,
            &ApiResponse::Event {
                stream: EventStream::Stdout,
                data: serde_json::to_string(&envelope).unwrap(),
            },
        )
        .unwrap();
        drop(writer);
        let root = tempfile::tempdir().unwrap();
        let mut app = crate::app::verification::fixture(root.path(), 0);
        let mut received = 0;
        let mut ended = false;
        wait(|| {
            match subscriptions.poll() {
                Some(Some(event @ TuiEvent::Structured { .. })) => {
                    app.apply_event(event);
                    received += 1;
                }
                Some(Some(TuiEvent::Ended { .. })) => ended = true,
                Some(Some(_)) => panic!("raw conversation output leaked"),
                _ => {}
            }
            ended
        });
        assert_eq!(received, 1);
        assert!(app.threads[0].metrics.is_empty());
        assert!(app.threads[0]
            .items
            .iter()
            .all(|i| !matches!(i.kind, ItemKind::Reply | ItemKind::Error)));
    }

    fn wait(mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready() {
            assert!(Instant::now() < deadline, "subscription test timed out");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn attach(subscriptions: &mut Subscriptions) -> UnixStream {
        let (reader, writer) = UnixStream::pair().unwrap();
        writer
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        subscriptions
            .start_with("worker".into(), move || Ok(reader), Vec::new(), false)
            .unwrap();
        writer
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = BufReader::new(writer.try_clone().unwrap());
        assert!(
            matches!(tachyon_api::transport::read_request(&mut request).unwrap(),
            ApiRequest::AgentSubscribe { id } if id == "worker")
        );
        writer
    }

    fn reply(text: String, final_reply: bool) -> AgentEvent {
        if final_reply {
            AgentEvent::Reply {
                turn: Some(1),
                text,
                final_reply: true,
            }
        } else {
            AgentEvent::ReplyDelta {
                turn: Some(1),
                text,
            }
        }
    }

    fn write(writer: &mut UnixStream, event: &AgentEvent) {
        write_response(
            writer,
            &ApiResponse::Event {
                stream: EventStream::Stdout,
                data: serde_json::to_string(event).unwrap(),
            },
        )
        .unwrap();
    }

    #[test]
    fn socket_flood_keeps_exact_order_and_authoritative_final_through_app_reducer() {
        let mut subscriptions = Subscriptions::default();
        let mut writer = attach(&mut subscriptions);
        let producer = std::thread::spawn(move || {
            for index in 0..1024 {
                write(&mut writer, &reply(format!("{index},"), false));
            }
            write(&mut writer, &reply("authoritative final".into(), true));
        });
        wait(|| subscriptions.incoming.usage().0 == EVENT_CAPACITY);
        assert!(subscriptions.incoming.usage().1 <= PAYLOAD_BUDGET);
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "Question".into(), None);
        thread.reserve_reply();
        crate::app::accept_user_turn(&mut thread, "Question", Some("1".into()));
        let mut received = 0;
        let mut ended = false;
        wait(|| {
            for _ in crate::app::scheduler::budget() {
                match subscriptions.poll() {
                    Some(Some(TuiEvent::Structured { envelope, .. })) => {
                        assert_eq!(
                            envelope.kind,
                            if received < 1024 {
                                reply(format!("{received},"), false)
                            } else {
                                reply("authoritative final".into(), true)
                            }
                        );
                        apply_correlated_agent_event(&mut thread, envelope.kind, None);
                        received += 1;
                    }
                    Some(Some(TuiEvent::Ended { agent_id, .. })) => {
                        subscriptions.end(&agent_id);
                        ended = true;
                    }
                    None => break,
                    _ => panic!("unexpected subscription event"),
                }
            }
            ended
        });
        producer.join().unwrap();
        assert_eq!(received, 1025);
        assert_eq!(
            thread
                .items
                .iter()
                .filter(|item| item.kind == ItemKind::Reply)
                .map(|item| item.text.as_str())
                .collect::<Vec<_>>(),
            vec!["authoritative final"]
        );
        assert!(thread.completed_turns.contains("1"));
    }

    #[test]
    fn stopped_consumer_wakes_full_sender_and_partial_frame_reader_without_ui_join() {
        for full in [false, true] {
            let mut subscriptions = Subscriptions::default();
            let mut writer = attach(&mut subscriptions);
            if full {
                for _ in 0..=EVENT_CAPACITY {
                    write(&mut writer, &reply("delta".into(), false));
                }
                wait(|| subscriptions.incoming.usage().0 == EVENT_CAPACITY);
            } else {
                writer.write_all(b"{\"event\":").unwrap();
            }
            subscriptions.stop();
            assert!(subscriptions.active.is_empty());
            assert_eq!(subscriptions.retired.len(), 1); // stop did not join
            wait(|| subscriptions.retired[0].thread.is_finished());
            subscriptions.reap();
            assert!(subscriptions.retired.is_empty());
        }
    }

    #[test]
    fn reconnect_rejects_old_queued_final_and_ended_without_overwriting_new_subscription() {
        let mut subscriptions = Subscriptions::default();
        let mut old = attach(&mut subscriptions);
        write(&mut old, &reply("stale final".into(), true));
        drop(old);
        wait(|| subscriptions.incoming.usage().0 == 2);
        let old_generation = subscriptions.active["worker"].generation;
        let mut new = attach(&mut subscriptions);
        assert_ne!(subscriptions.active["worker"].generation, old_generation);
        write(&mut new, &reply("new final".into(), true));
        assert!(matches!(subscriptions.poll(), Some(None)));
        assert!(matches!(subscriptions.poll(), Some(None)));
        assert!(subscriptions.contains("worker")); // stale Ended cannot remove it
        let mut received = None;
        wait(|| {
            if let Some(Some(TuiEvent::Structured { envelope, .. })) = subscriptions.poll() {
                received = Some(envelope.kind);
            }
            received.is_some()
        });
        assert_eq!(received, Some(reply("new final".into(), true)));
        assert!(subscriptions.contains("worker"));
    }

    #[test]
    fn cancellation_before_socket_registration_shuts_down_late_socket() {
        let sockets = Sockets(Mutex::new(Some(Vec::new())));
        sockets.stop();
        let (reader, mut peer) = UnixStream::pair().unwrap();
        assert!(sockets.register(reader).is_err());
        assert!(peer.write_all(b"late").is_err());
    }

    #[test]
    fn oversized_valid_tool_output_is_not_truncated_or_disconnected() {
        let mut subscriptions = Subscriptions::default();
        let mut writer = attach(&mut subscriptions);
        let expected = AgentEvent::ToolFinished {
            turn: Some(1),
            id: "tool".into(),
            output: "x".repeat(PAYLOAD_BUDGET + 1),
            identity: None,
        };
        let event = expected.clone();
        let producer = std::thread::spawn(move || {
            write(&mut writer, &event);
            write(&mut writer, &reply("final after tool".into(), true));
        });
        wait(|| subscriptions.incoming.usage().1 > PAYLOAD_BUDGET);
        assert_eq!(subscriptions.incoming.usage().0, 1);
        let Some(Some(TuiEvent::Structured { envelope, .. })) = subscriptions.poll() else {
            panic!("missing oversized tool result");
        };
        assert_eq!(envelope.kind, expected);
        let mut final_received = false;
        wait(|| {
            if let Some(Some(TuiEvent::Structured { envelope, .. })) = subscriptions.poll() {
                assert_eq!(envelope.kind, reply("final after tool".into(), true));
                final_received = true;
            }
            final_received
        });
        producer.join().unwrap();
    }
}
