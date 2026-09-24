use super::*;
use std::{
    io::BufReader,
    os::unix::net::UnixListener,
    sync::atomic::{AtomicU64, Ordering},
};
use tachyon_api::{
    interaction_manager::*,
    transport::{read_request, write_response},
};

fn fixture(
    run: impl FnOnce(std::os::unix::net::UnixStream) + Send + 'static,
) -> (Client, std::thread::JoinHandle<()>) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = format!(
        "/tmp/opencode/interaction-client-{}-{}.sock",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let listener = UnixListener::bind(&path).unwrap();
    let client = Client {
        conn: Connection::connect(&path).unwrap(),
    };
    let join = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        std::fs::remove_file(path).unwrap();
        run(socket);
    });
    (client, join)
}

fn snapshot() -> Snapshot {
    Snapshot {
        revision: Revision {
            epoch: "host".into(),
            sequence: 7,
        },
        conversation_id: "foreground".into(),
        session_id: Some("real-session".into()),
        host_state: None,
        history: vec![],
        history_content: vec![],
        projection: Projection::default(),
        projection_next: Some(200),
    }
}

#[test]
fn projection_pages_require_exact_revision_and_do_not_publish_partial_assembly() {
    for gap in [false, true] {
        let (mut client, join) = fixture(move |mut socket| {
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let ApiRequest::InteractionProjection { revision, offset } =
                read_request(&mut reader).unwrap()
            else {
                panic!()
            };
            assert_eq!(revision, snapshot().revision);
            assert_eq!(offset, 200);
            let response = if gap {
                ApiResponse::InteractionFrame {
                    frame: Frame::ResnapshotRequired {
                        current: Revision {
                            epoch: "new".into(),
                            sequence: 0,
                        },
                    },
                }
            } else {
                ApiResponse::InteractionProjection {
                    page: ProjectionPage {
                        revision,
                        projection: Projection::default(),
                        next_offset: None,
                    },
                }
            };
            write_response(&mut socket, &response).unwrap();
        });
        let result = client.assemble_interaction_snapshot(snapshot());
        assert_eq!(result.is_err(), gap);
        if let Ok(snapshot) = result {
            assert!(snapshot.projection_next.is_none());
        }
        join.join().unwrap();
    }
}

#[test]
fn content_reference_is_opaque_utf8_pages_are_joined_and_snapshot_byte_bound_wins() {
    let (mut client, join) = fixture(|mut socket| {
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        for (wanted, bytes) in [(0, vec![0xe2]), (1, vec![0x82, 0xac])] {
            let ApiRequest::InteractionContent {
                reference,
                offset,
                limit,
            } = read_request(&mut reader).unwrap()
            else {
                panic!()
            };
            assert_eq!(reference, "opaque:content");
            assert_eq!(offset, wanted);
            assert_eq!(limit, Some(3 - wanted as usize));
            write_response(
                &mut socket,
                &ApiResponse::InteractionContent {
                    page: ContentPage {
                        reference,
                        offset,
                        next_offset: Some(offset + bytes.len() as u64),
                        bytes,
                    },
                },
            )
            .unwrap();
        }
    });
    assert_eq!(
        client.interaction_text("opaque:content", 3).unwrap(),
        "\u{20ac}"
    );
    join.join().unwrap();
}
