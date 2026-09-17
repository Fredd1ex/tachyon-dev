use std::future::Future;

use tachyon_api::campaign_oversight::{BackgroundRequest, BackgroundResponse};
use tachyon_api::BackgroundScheduleRequest;
#[cfg(test)]
use tachyon_api::{WorkReviewDecision, WorkReviewRequest};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinSet;

use super::scheduling::review_schedule_request;

const MAX_CONCURRENT_REVIEWS: usize = 4;
const MAX_REQUEST_BYTES: usize = 256 * 1024;

// Keep partial input outside the future: select! may cancel it when a review finishes.
async fn next_line<R: AsyncRead + Unpin>(
    reader: &mut tokio::io::BufReader<R>,
    pending: &mut Vec<u8>,
) -> std::io::Result<Option<Vec<u8>>> {
    loop {
        let bytes = reader.fill_buf().await?;
        if bytes.is_empty() {
            return Ok((!pending.is_empty()).then(|| std::mem::take(pending)));
        }
        let end = bytes.iter().position(|b| *b == b'\n');
        let count = end.map_or(bytes.len(), |at| at + 1);
        if pending.len() + count > MAX_REQUEST_BYTES {
            return Err(std::io::Error::other("background request too large"));
        }
        pending.extend_from_slice(&bytes[..count]);
        reader.consume(count);
        if end.is_some() {
            return Ok(Some(std::mem::take(pending)));
        }
    }
}

// Shared API observations are permissive. At this untrusted inference boundary,
// reject fields that deserialization would otherwise silently discard, at any depth.
fn decode<T: serde::de::DeserializeOwned + serde::Serialize>(
    line: &[u8],
) -> Result<T, serde_json::Error> {
    fn known(input: &serde_json::Value, output: &serde_json::Value) -> bool {
        match (input, output) {
            (serde_json::Value::Object(a), serde_json::Value::Object(b)) => a
                .iter()
                .all(|(key, value)| b.get(key).is_some_and(|other| known(value, other))),
            (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| known(a, b))
            }
            _ => true,
        }
    }
    let value: serde_json::Value = serde_json::from_slice(line)?;
    let request: T = serde_json::from_slice(line)?;
    if !known(&value, &serde_json::to_value(&request)?) {
        return Err(<serde_json::Error as serde::de::Error>::custom(
            "unknown request field",
        ));
    }
    Ok(request)
}

async fn write_decision<W: AsyncWrite + Unpin>(
    writer: &mut W,
    decision: &impl serde::Serialize,
) -> std::io::Result<()> {
    let mut encoded = serde_json::to_vec(decision).map_err(std::io::Error::other)?;
    encoded.push(b'\n');
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        writer.write_all(&encoded).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "background output stalled"))?
}

pub(super) async fn process_requests<R, W, F, Fut>(
    reader: R,
    mut writer: W,
    review_fn: F,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    F: Fn(BackgroundRequest) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = BackgroundResponse> + Send + 'static,
{
    let mut reader = tokio::io::BufReader::new(reader);
    let mut pending = Vec::new();
    let mut reviews = JoinSet::new();
    loop {
        if reviews.len() == MAX_CONCURRENT_REVIEWS {
            write_next_decision(&mut reviews, &mut writer).await?;
            continue;
        }
        tokio::select! {
            result = reviews.join_next(), if !reviews.is_empty() => {
                write_joined_decision(result, &mut writer).await?;
            }
            line = next_line(&mut reader, &mut pending) => {
                let Some(line) = line? else { break };
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                if let Ok(request) = decode::<BackgroundScheduleRequest>(&line) {
                    let decision = review_schedule_request(request);
                    write_decision(&mut writer, &decision).await?;
                    continue;
                }
                let request = match serde_json::from_slice::<BackgroundRequest>(&line).and_then(|request| {
                    match request {
                        BackgroundRequest::CampaignAssessment(_) => decode(&line).map(BackgroundRequest::CampaignAssessment),
                        review @ BackgroundRequest::Review(_) => Ok(review),
                    }
                }) {
                    Ok(request) => request,
                    Err(error) => {
                        eprintln!("tachyon-background: invalid review request: {error}");
                        continue;
                    }
                };
                let review_fn = review_fn.clone();
                reviews.spawn(async move { review_fn(request).await });
            }
        }
    }
    while !reviews.is_empty() {
        write_next_decision(&mut reviews, &mut writer).await?;
    }
    Ok(())
}

async fn write_next_decision<W: AsyncWrite + Unpin>(
    reviews: &mut JoinSet<BackgroundResponse>,
    writer: &mut W,
) -> std::io::Result<()> {
    let result = reviews
        .join_next()
        .await
        .ok_or_else(|| std::io::Error::other("review task set was empty"))?;
    write_joined_decision(Some(result), writer).await
}

async fn write_joined_decision<W: AsyncWrite + Unpin>(
    result: Option<Result<BackgroundResponse, tokio::task::JoinError>>,
    writer: &mut W,
) -> std::io::Result<()> {
    let decision = result
        .ok_or_else(|| std::io::Error::other("review task set was empty"))?
        .map_err(std::io::Error::other)?;
    write_decision(writer, &decision).await
}

#[cfg(test)]
async fn process_reviews<R, W, F, Fut>(reader: R, writer: W, review_fn: F) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    F: Fn(WorkReviewRequest) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = WorkReviewDecision> + Send + 'static,
{
    process_requests(reader, writer, move |request| {
        let BackgroundRequest::Review(request) = request else {
            panic!("expected review")
        };
        let result = review_fn(request.request);
        async move { BackgroundResponse::Review(result.await) }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tachyon_api::{
        LifetimeClass, WorkOutcome, WorkResult, WorkReviewContext, WorkReviewFailure,
        WorkReviewRecommendation,
    };
    use tokio::sync::{mpsc, Semaphore};

    #[tokio::test]
    async fn partial_frame_survives_cancellation_and_oversized_input_is_bounded() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut reader = tokio::io::BufReader::new(server);
        let mut pending = Vec::new();
        client.write_all(b"partial").await.unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(20),
            next_line(&mut reader, &mut pending)
        )
        .await
        .is_err());
        assert_eq!(pending, b"partial");
        client.write_all(b" rest\n").await.unwrap();
        assert_eq!(
            next_line(&mut reader, &mut pending).await.unwrap().unwrap(),
            b"partial rest\n"
        );
        let bytes = vec![b' '; MAX_REQUEST_BYTES + 1];
        let mut reader = tokio::io::BufReader::new(bytes.as_slice());
        assert!(next_line(&mut reader, &mut pending).await.is_err());
        assert!(pending.len() <= MAX_REQUEST_BYTES);
    }

    #[tokio::test]
    async fn stalled_output_returns_instead_of_hanging() {
        let (mut writer, _unread) = tokio::io::duplex(1);
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(4),
            write_decision(&mut writer, &"blocked"),
        )
        .await
        .expect("output timeout did not fire")
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn tagged_or_unknown_fields_cannot_fall_back_to_schedule() {
        for input in [
            r#"{"request_id":"s","action":{"action":"list"},"kind":"campaign_assessment"}"#,
            r#"{"request_id":"s","action":{"action":"list","extra":true}}"#,
        ] {
            assert!(decode::<BackgroundScheduleRequest>(input.as_bytes()).is_err());
            assert!(decode::<BackgroundRequest>(input.as_bytes()).is_err());
        }
        assert!(decode::<BackgroundScheduleRequest>(
            br#"{"request_id":"s","action":{"action":"list"}}"#
        )
        .is_ok());
    }

    #[tokio::test]
    async fn review_stream_runs_with_a_small_bound_and_preserves_correlation() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (service, client) = tokio::io::duplex(16_384);
            let (service_reader, service_writer) = tokio::io::split(service);
            let (client_reader, mut client_writer) = tokio::io::split(client);
            let gates = Arc::new((0..6).map(|_| Semaphore::new(0)).collect::<Vec<_>>());
            let (started_tx, mut started_rx) = mpsc::unbounded_channel();
            let service_gates = Arc::clone(&gates);
            let service = tokio::spawn(process_reviews(
                service_reader,
                service_writer,
                move |request| {
                    let gates = Arc::clone(&service_gates);
                    let started_tx = started_tx.clone();
                    async move {
                        let index = request.review_id.parse::<usize>().unwrap();
                        started_tx.send(index).unwrap();
                        gates[index].acquire().await.unwrap().forget();
                        WorkReviewDecision {
                            review_id: request.review_id,
                            coordinator_generation: request.coordinator_generation,
                            work_id: request.candidate.work_id,
                            generation: request.candidate.generation,
                            assignment: request.candidate.assignment,
                            recommendation: WorkReviewRecommendation::Inconclusive {
                                failure: WorkReviewFailure::ProviderError,
                            },
                            rationale: "test".into(),
                        }
                    }
                },
            ));

            assert_eq!(MAX_CONCURRENT_REVIEWS, 4);
            let mut expected = Vec::new();
            for index in 0..6 {
                let request = WorkReviewRequest {
                    review_id: index.to_string(),
                    coordinator_generation: 10 + index as u64,
                    candidate: WorkResult {
                        attempt_id: None,
                        candidate_refs: None,
                        final_context: None,
                        instruction_revision: None,
                        work_id: format!("work-{index}"),
                        evidence: Default::default(),
                        timing: None,
                        objective: "verify release".into(),
                        generation: 20 + index as u64,
                        assignment: 30 + index as u64,
                        outcome: WorkOutcome::Completed {
                            result: "verified".into(),
                            artifacts: Vec::new(),
                            context: String::new(),
                            suggested_reuse: false,
                        },
                    },
                    worker: WorkReviewContext {
                        worker_id: "worker-1".into(),
                        current_lifetime_class: LifetimeClass::Short,
                        turns_used: 1,
                        turn_budget: Some(3),
                        purpose: "research".into(),
                    },
                    deadline_ms: u64::MAX,
                };
                expected.push(WorkReviewDecision {
                    review_id: index.to_string(),
                    coordinator_generation: 10 + index as u64,
                    work_id: format!("work-{index}"),
                    generation: 20 + index as u64,
                    assignment: 30 + index as u64,
                    recommendation: WorkReviewRecommendation::Inconclusive {
                        failure: WorkReviewFailure::ProviderError,
                    },
                    rationale: "test".into(),
                });
                let wire = serde_json::to_string(&request).unwrap();
                let decoded: BackgroundRequest = serde_json::from_str(&wire).unwrap();
                assert_eq!(serde_json::to_string(&decoded).unwrap(), wire);
                // A malformed tagged request must not fall back to a review.
                let tagged = wire.replacen('{', "{\"kind\":\"campaign_assessment\",", 1);
                assert!(serde_json::from_str::<BackgroundRequest>(&tagged).is_err());
                // Explicit nulls in the shipped review wire remain legal even though
                // its serializer omits them. Campaign strictness must not change that.
                let mut input = serde_json::to_value(&request).unwrap();
                input["candidate"]["timing"] = serde_json::Value::Null;
                client_writer
                    .write_all(&serde_json::to_vec(&input).unwrap())
                    .await
                    .unwrap();
                client_writer.write_all(b"\n").await.unwrap();
                if index == 0 {
                    // B is only sent after A has entered its blocked review.
                    assert_eq!(started_rx.recv().await, Some(0));
                }
            }
            client_writer.shutdown().await.unwrap();

            let mut started = vec![0];
            for _ in 1..MAX_CONCURRENT_REVIEWS {
                started.push(started_rx.recv().await.unwrap());
            }
            started.sort_unstable();
            assert_eq!(started, vec![0, 1, 2, 3]);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(50), started_rx.recv())
                    .await
                    .is_err()
            );

            let mut output = tokio::io::BufReader::new(client_reader).lines();
            for index in [1, 4, 5, 3, 2, 0] {
                gates[index].add_permits(1);
                let line = output.next_line().await.unwrap().unwrap();
                let decision: WorkReviewDecision = serde_json::from_str(&line).unwrap();
                assert_eq!(decision, expected[index]);
                if index == 1 || index == 4 {
                    assert_eq!(
                        started_rx.recv().await,
                        Some(if index == 1 { 4 } else { 5 })
                    );
                    assert!(tokio::time::timeout(
                        std::time::Duration::from_millis(50),
                        started_rx.recv()
                    )
                    .await
                    .is_err());
                }
            }
            service.await.unwrap().unwrap();
            assert!(output.next_line().await.unwrap().is_none());
        })
        .await
        .expect("review stream stalled");
    }
}
