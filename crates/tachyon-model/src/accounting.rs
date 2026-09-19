//! Opt-in, host-authorized accounting for one actual model request.
use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};

use crate::{ModelError, Result};

pub type AccountingFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestClass {
    Work,
    Compaction,
    Verification,
}

/// A work objective survives retries. attempt_id identifies the host's execution
/// attempt, not an admission command or a provider request reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkIdentity {
    pub campaign_id: String,
    pub work_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub instruction_revision: u64,
    pub class: RequestClass,
}

/// Host-attested bounds for the complete wire request, including tools, reasoning
/// and cache charges. These are NOT tokenizer estimates or live price discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestEstimate {
    pub base_url: String,
    pub model: String,
    pub provider: String,
    pub pricing_revision: String,
    pub max_request_bytes: u64,
    pub input_tokens: u64,
    pub output_tokens: u32,
    pub input_micro_usd_per_million: u64,
    pub output_micro_usd_per_million: u64,
    /// Upper bound for all charges not covered by the two token rates, including
    /// every permitted server web-tool use. Web callers must supply a host-attested
    /// ceiling covering the fixed tool's fee floor; inclusive usage.cost is
    /// reconciled without adding fees.
    pub other_micro_usd: u64,
}

impl RequestEstimate {
    pub fn upper_bound(&self) -> Result<(u64, u64)> {
        if self.base_url.is_empty()
            || self.model.is_empty()
            || self.provider.is_empty()
            || self.pricing_revision.is_empty()
            || self.max_request_bytes == 0
            || self.input_tokens == 0
            || self.output_tokens == 0
        {
            return Err(ModelError::Accounting(
                "missing host request bounds/pricing".into(),
            ));
        }
        let tokens = self.input_tokens.checked_add(u64::from(self.output_tokens));
        let cost = (u128::from(self.input_tokens) * u128::from(self.input_micro_usd_per_million))
            .checked_add(
                u128::from(self.output_tokens) * u128::from(self.output_micro_usd_per_million),
            )
            .ok_or_else(|| ModelError::Accounting("price bound overflow".into()))?
            .div_ceil(1_000_000)
            + u128::from(self.other_micro_usd);
        Ok((
            tokens.ok_or_else(|| ModelError::Accounting("token bound overflow".into()))?,
            u64::try_from(cost)
                .map_err(|_| ModelError::Accounting("cost bound overflow".into()))?,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestReservation {
    pub identity: WorkIdentity,
    pub estimate: RequestEstimate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestUsage {
    Unknown,
    /// Inclusive provider billing cost, rounded UP to integer microUSD. Never
    /// derive final cost from uncached token prices or display TokenUsage.
    Final {
        input_tokens: u64,
        output_tokens: u64,
        cost_micro_usd: u64,
    },
}

/// Each successful reserve must atomically commit an unresolved hold and exclusive
/// dispatch claim before returning its receipt. An implementation may claim a
/// previously reserved, unclaimed hold, but MUST reject repeated dispatch claims:
/// returning an idempotent reservation receipt alone would execute the provider
/// twice. Cancellation/drop after reserve retains the hold and claim.
/// Implementations must not hold locks/transactions across await.
pub trait RequestAccounting: Send + Sync {
    fn reserve<'a>(&'a self, request: &'a RequestReservation) -> AccountingFuture<'a, String>;
    fn reconcile<'a>(&'a self, receipt: &'a str, usage: RequestUsage) -> AccountingFuture<'a, ()>;
}

pub struct AccountingContext<'a> {
    pub accountant: &'a dyn RequestAccounting,
    pub request: RequestReservation,
}

/// The provider future is not even constructed until reservation succeeds.
/// Dropping before final reconciliation leaves the durable hold unresolved.
pub(crate) async fn dispatch<T, F, Fut>(
    context: Option<&AccountingContext<'_>>,
    provider: F,
) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(T, RequestUsage)>>,
{
    let receipt = match context {
        Some(context) => Some(context.accountant.reserve(&context.request).await?),
        None => None,
    };
    let outcome = provider().await;
    if let (Some(context), Some(receipt)) = (context, receipt) {
        let usage = outcome
            .as_ref()
            .map(|(_, usage)| *usage)
            .unwrap_or(RequestUsage::Unknown);
        context.accountant.reconcile(&receipt, usage).await?;
    }
    outcome.map(|(result, _)| result)
}

/// Strict OpenRouter inclusive billing evidence. Missing, malformed, fractional
/// tokens or unsupported cost representations remain unknown, never final zero.
pub(crate) fn openrouter_usage(value: &serde_json::Value) -> Option<RequestUsage> {
    if value
        .get("is_byok")
        .is_some_and(|v| v != &serde_json::Value::Bool(false))
    {
        return None;
    }
    let input = value.get("prompt_tokens")?.as_u64()?;
    let output = value.get("completion_tokens")?.as_u64()?;
    if value.get("total_tokens")?.as_u64()? != input.checked_add(output)? {
        return None;
    }
    let cost = value.get("cost")?.as_number()?.to_string();
    let (mantissa, exponent) = match cost.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
        None => (cost.as_str(), 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Shift the decimal point, not a float or an exponent-sized allocation.
    // Discarded nonzero digits round UP, including sub-microUSD charges.
    let integer_digits = i64::try_from(whole.len())
        .ok()?
        .checked_add(exponent)?
        .checked_add(6)?;
    let mut micros = 0u64;
    let mut round_up = false;
    for (index, digit) in whole.bytes().chain(fraction.bytes()).enumerate() {
        if i64::try_from(index).ok()? < integer_digits {
            micros = micros
                .checked_mul(10)?
                .checked_add(u64::from(digit - b'0'))?;
        } else {
            round_up |= digit != b'0';
        }
    }
    let padding = integer_digits.saturating_sub(i64::try_from(whole.len() + fraction.len()).ok()?);
    if micros != 0 && padding > 0 {
        micros = micros.checked_mul(10u64.checked_pow(u32::try_from(padding).ok()?)?)?;
    }
    micros = micros.checked_add(u64::from(round_up))?;
    Some(RequestUsage::Final {
        input_tokens: input,
        output_tokens: output,
        cost_micro_usd: micros,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::sync::Mutex;

    fn request() -> RequestReservation {
        RequestReservation {
            identity: WorkIdentity {
                campaign_id: "campaign".into(),
                work_id: "objective".into(),
                attempt_id: "execution".into(),
                generation: 1,
                instruction_revision: 1,
                class: RequestClass::Work,
            },
            estimate: RequestEstimate {
                base_url: "https://example.invalid".into(),
                model: "fake".into(),
                provider: "fake".into(),
                pricing_revision: "host-policy-1".into(),
                max_request_bytes: 10000,
                input_tokens: 100,
                output_tokens: 20,
                input_micro_usd_per_million: 1_000_000,
                output_micro_usd_per_million: 2_000_000,
                other_micro_usd: 10,
            },
        }
    }

    struct FakeAccounting {
        limit: usize,
        holds: Mutex<Vec<RequestUsage>>,
    }
    impl RequestAccounting for FakeAccounting {
        fn reserve<'a>(&'a self, _: &'a RequestReservation) -> AccountingFuture<'a, String> {
            Box::pin(async {
                let mut holds = self.holds.lock().unwrap();
                if holds.len() == self.limit {
                    return Err(ModelError::Accounting("denied".into()));
                }
                let receipt = holds.len().to_string();
                holds.push(RequestUsage::Unknown);
                Ok(receipt)
            })
        }
        fn reconcile<'a>(
            &'a self,
            receipt: &'a str,
            usage: RequestUsage,
        ) -> AccountingFuture<'a, ()> {
            Box::pin(async move {
                self.holds.lock().unwrap()[receipt.parse::<usize>().unwrap()] = usage;
                Ok(())
            })
        }
    }

    #[test]
    fn denied_never_constructs_provider_and_retries_get_separate_holds() {
        let accountant = FakeAccounting {
            limit: 2,
            holds: Mutex::new(vec![]),
        };
        let context = AccountingContext {
            accountant: &accountant,
            request: request(),
        };
        let failure = dispatch::<(), _, _>(Some(&context), || {
            // Assert at future construction, not just when it is first polled.
            assert_eq!(accountant.holds.lock().unwrap().len(), 1);
            async { Err(ModelError::Api("timeout: spend unknown".into())) }
        })
        .now_or_never()
        .unwrap();
        assert!(failure.is_err());
        let actual = RequestUsage::Final {
            input_tokens: 30,
            output_tokens: 7,
            cost_micro_usd: 55,
        };
        dispatch(Some(&context), || async { Ok(((), actual)) })
            .now_or_never()
            .unwrap()
            .unwrap();
        assert!(dispatch::<(), _, _>(Some(&context), || {
            panic!("denied request dispatched");
            #[allow(unreachable_code)]
            async {
                Ok(((), RequestUsage::Unknown))
            }
        })
        .now_or_never()
        .unwrap()
        .is_err());
        assert_eq!(
            *accountant.holds.lock().unwrap(),
            vec![RequestUsage::Unknown, actual]
        );
    }

    #[test]
    fn dropped_provider_future_keeps_unresolved_hold() {
        let accountant = FakeAccounting {
            limit: 1,
            holds: Mutex::new(vec![]),
        };
        let context = AccountingContext {
            accountant: &accountant,
            request: request(),
        };
        assert!(
            dispatch::<(), _, _>(Some(&context), || std::future::pending())
                .now_or_never()
                .is_none()
        );
        assert_eq!(
            *accountant.holds.lock().unwrap(),
            vec![RequestUsage::Unknown]
        );
    }

    #[test]
    fn public_model_boundary_denies_before_http_and_checks_wire_bounds() {
        let request = request();
        let model = crate::Model::new(crate::ModelConfig {
            base_url: request.estimate.base_url.clone(),
            api_key: "fake-not-a-credential".into(),
            model: request.estimate.model.clone(),
            temperature: 0.0,
            max_completion_tokens: Some(request.estimate.output_tokens),
            context_length: None,
            parallel_tool_calls: false,
            reasoning: crate::Reasoning::default(),
            routing: None,
            debug: false,
            debug_log: None,
        });
        let accountant = FakeAccounting {
            limit: 0,
            holds: Mutex::new(vec![]),
        };
        let mut context = AccountingContext {
            accountant: &accountant,
            request,
        };
        let mut sink = |_: &str| {};
        let result = model
            .chat_accounted(&[], None, None, &mut sink, &context)
            .now_or_never()
            .expect("denial must not poll network I/O");
        assert!(matches!(result, Err(ModelError::Accounting(ref error)) if error == "denied"));
        context.request.estimate.max_request_bytes = 1;
        let result = model
            .chat_accounted(&[], None, None, &mut sink, &context)
            .now_or_never()
            .unwrap();
        assert!(
            matches!(result, Err(ModelError::Accounting(ref error)) if error.contains("byte bound"))
        );
        assert!(accountant.holds.lock().unwrap().is_empty());
        context.request.estimate.max_request_bytes = 10000;
        context.request.estimate.output_tokens += 1;
        let result = model
            .chat_accounted(&[], None, None, &mut sink, &context)
            .now_or_never()
            .unwrap();
        assert!(
            matches!(result, Err(ModelError::Accounting(ref error)) if error.contains("host bounds"))
        );
        assert!(accountant.holds.lock().unwrap().is_empty());
    }

    #[test]
    fn inclusive_cost_uses_actual_input_output_and_never_defaults_malformed_to_zero() {
        let parse = |text: &str| openrouter_usage(&serde_json::from_str(text).unwrap());
        assert_eq!(
            parse(
                r#"{"prompt_tokens":30,"completion_tokens":7,"total_tokens":37,"cost":0.0000551,"prompt_tokens_details":{"cached_tokens":20,"cache_write_tokens":5}}"#
            ),
            Some(RequestUsage::Final {
                input_tokens: 30,
                output_tokens: 7,
                cost_micro_usd: 56
            })
        );
        for text in [
            "{}",
            r#"{"prompt_tokens":30,"completion_tokens":7,"total_tokens":37}"#,
            r#"{"prompt_tokens":30,"completion_tokens":7,"total_tokens":36,"cost":0}"#,
            r#"{"prompt_tokens":30,"completion_tokens":null,"total_tokens":30,"cost":0}"#,
            r#"{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"cost":-1}"#,
            r#"{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"cost":"0"}"#,
            r#"{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"cost":0,"is_byok":true}"#,
        ] {
            assert_eq!(parse(text), None, "{text}");
        }
        assert_eq!(
            parse(r#"{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"cost":0}"#),
            Some(RequestUsage::Final {
                input_tokens: 0,
                output_tokens: 0,
                cost_micro_usd: 0
            })
        );
        // More than f64 precision must not round a charge down to zero.
        assert_eq!(
            parse(
                r#"{"prompt_tokens":1,"completion_tokens":0,"total_tokens":1,"cost":0.00000100000000000000000001}"#
            ),
            Some(RequestUsage::Final {
                input_tokens: 1,
                output_tokens: 0,
                cost_micro_usd: 2
            })
        );
    }

    #[test]
    fn host_bounds_are_checked_integer_arithmetic_not_display_estimates() {
        let mut estimate = request().estimate;
        assert_eq!(estimate.upper_bound().unwrap(), (120, 150));
        estimate.input_tokens = u64::MAX;
        assert!(estimate.upper_bound().is_err());
        estimate.output_tokens = u32::MAX;
        estimate.input_micro_usd_per_million = u64::MAX;
        estimate.output_micro_usd_per_million = u64::MAX;
        assert!(estimate.upper_bound().is_err());
    }

    #[test]
    fn decimal_billing_is_exact_bounded_and_includes_cache_charges() {
        for (cost, expected) in [
            ("1e-7", Some(1)),
            ("1.00000000000000000001e-6", Some(2)),
            ("5.5E-5", Some(55)),
            ("1e2", Some(100_000_000)),
            ("1e-1000000", Some(1)),
            ("0e1000000", Some(0)),
            ("1e1000000", None),
            ("18446744073709.551615", Some(u64::MAX)),
            ("18446744073709.5516151", None),
            ("-0.1", None),
        ] {
            let raw = format!(
                r#"{{"prompt_tokens":30,"completion_tokens":7,"total_tokens":37,"cost":{cost},"prompt_tokens_details":{{"cached_tokens":20,"cache_write_tokens":5}}}}"#
            );
            assert_eq!(
                openrouter_usage(&serde_json::from_str(&raw).unwrap()),
                expected.map(|cost_micro_usd| RequestUsage::Final {
                    input_tokens: 30,
                    output_tokens: 7,
                    cost_micro_usd,
                }),
                "{cost}"
            );
        }
        let mut estimate = request().estimate;
        estimate.input_tokens = u64::MAX - u64::from(estimate.output_tokens);
        estimate.input_micro_usd_per_million = u64::MAX;
        estimate.output_micro_usd_per_million = u64::MAX;
        estimate.other_micro_usd = u64::MAX;
        assert!(estimate.upper_bound().is_err());
    }

    async fn http_server(
        body: Option<String>,
    ) -> (
        String,
        tokio::sync::oneshot::Receiver<serde_json::Value>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(socket.read_u8().await.unwrap());
                assert!(headers.len() < 16384);
            }
            let headers = String::from_utf8(headers).unwrap();
            assert!(headers.starts_with("POST /chat/completions HTTP/1.1\r\n"));
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            assert!(length < 10000);
            let mut bytes = vec![0; length];
            socket.read_exact(&mut bytes).await.unwrap();
            tx.send(serde_json::from_slice(&bytes).unwrap()).unwrap();
            if let Some(body) = body {
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            } else {
                std::future::pending::<()>().await;
            }
        });
        (url, rx, task)
    }

    fn http_model(request: &RequestReservation) -> crate::Model {
        crate::Model::new(crate::ModelConfig {
            base_url: request.estimate.base_url.clone(),
            api_key: "fake".into(),
            model: request.estimate.model.clone(),
            temperature: 0.0,
            max_completion_tokens: Some(request.estimate.output_tokens),
            context_length: None,
            parallel_tool_calls: false,
            reasoning: crate::Reasoning::default(),
            routing: Some(crate::ProviderRouting {
                allow_fallbacks: Some(true),
                ..Default::default()
            }),
            debug: false,
            debug_log: None,
        })
    }

    #[tokio::test]
    async fn actual_http_wire_usage_malformed_and_timeout() {
        let valid = r#"data: {"choices":[],"usage":{"prompt_tokens":30,"completion_tokens":7,"total_tokens":37,"cost":5.51e-5,"prompt_tokens_details":{"cached_tokens":20,"cache_write_tokens":5}}}

"#;
        for (body, final_usage) in [
            (Some(format!("{valid}data: [DONE]\n\n")), true),
            (Some(format!("data: {{\"usage\":{{}}}}\n\n{valid}data: [DONE]\n\n")), false),
            (Some(format!("{valid}data: {{\"usage\":{{}}}}\n\ndata: [DONE]\n\n")), false),
            (Some(format!("{valid}data: {{\"usage\":{{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0,\"cost\":0}}}}\n\ndata: [DONE]\n\n")), false),
            (Some(valid.to_string()), false),
            (None, false),
        ] {
            let times_out = body.is_none();
            let (url, wire, server) = http_server(body).await;
            let accountant = FakeAccounting { limit: 1, holds: Mutex::new(vec![]) };
            let mut request = request();
            request.estimate.base_url = url;
            let model = http_model(&request);
            let context = AccountingContext { accountant: &accountant, request };
            let mut sink = |_: &str| {};
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                model.chat_accounted(&[], None, None, &mut sink, &context),
            ).await;
            if times_out { assert!(result.is_err()); }
            if final_usage { assert!(result.unwrap().is_ok()); }
            let wire = wire.await.unwrap();
            assert_eq!(wire["max_tokens"], 20);
            assert!(wire.get("max_completion_tokens").is_none());
            assert_eq!(wire["provider"], serde_json::json!({
                "only": ["fake"], "allow_fallbacks": false, "require_parameters": true
            }));
            assert_eq!(wire["stream_options"]["include_usage"], true);
            assert!(wire.get("accounting").is_none());
            assert_eq!(*accountant.holds.lock().unwrap(), vec![if final_usage {
                RequestUsage::Final { input_tokens: 30, output_tokens: 7, cost_micro_usd: 56 }
            } else { RequestUsage::Unknown }]);
            assert!(accountant.reserve(&context.request).await.is_err());
            server.abort();
        }
    }

    #[tokio::test]
    async fn drop_after_http_response_before_reconciliation_retains_hold() {
        struct PausedAccounting {
            inner: FakeAccounting,
            reconciling: tokio::sync::Notify,
        }
        impl RequestAccounting for PausedAccounting {
            fn reserve<'a>(
                &'a self,
                request: &'a RequestReservation,
            ) -> AccountingFuture<'a, String> {
                self.inner.reserve(request)
            }
            fn reconcile<'a>(
                &'a self,
                _: &'a str,
                usage: RequestUsage,
            ) -> AccountingFuture<'a, ()> {
                Box::pin(async move {
                    assert!(matches!(usage, RequestUsage::Final { .. }));
                    self.reconciling.notify_one();
                    std::future::pending().await
                })
            }
        }
        let (url, wire, server) = http_server(Some("data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2,\"cost\":0}}\n\ndata: [DONE]\n\n".into())).await;
        let accountant = PausedAccounting {
            inner: FakeAccounting {
                limit: 1,
                holds: Mutex::new(vec![]),
            },
            reconciling: tokio::sync::Notify::new(),
        };
        let mut request = request();
        request.estimate.base_url = url;
        let model = http_model(&request);
        let context = AccountingContext {
            accountant: &accountant,
            request,
        };
        let mut sink = |_: &str| {};
        let mut call = Box::pin(model.chat_accounted(&[], None, None, &mut sink, &context));
        tokio::select! {
            _ = &mut call => panic!("reconciliation must remain pending"),
            _ = accountant.reconciling.notified() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => panic!("no response"),
        }
        drop(call);
        wire.await.unwrap();
        server.await.unwrap();
        assert_eq!(
            *accountant.inner.holds.lock().unwrap(),
            vec![RequestUsage::Unknown]
        );
        assert!(accountant.reserve(&context.request).await.is_err());
    }
}
