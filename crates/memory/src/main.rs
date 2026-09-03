#![forbid(unsafe_code)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use tachyon_memory::protocol::{MemoryRequest, MemoryResponse};
use tachyon_memory::MemoryStore;

fn main() -> std::io::Result<()> {
    let (database, socket, initialize) = args();
    let store = MemoryStore::open(database).map_err(std::io::Error::other)?;
    if initialize {
        return Ok(());
    }
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_connection(stream, &store),
            Err(error) => eprintln!("tachyon-memory: connection: {error}"),
        }
    }
    Ok(())
}

fn handle_connection(stream: UnixStream, store: &MemoryStore) {
    let reader = match stream.try_clone() {
        Ok(stream) => BufReader::new(stream),
        Err(error) => {
            eprintln!("tachyon-memory: connection clone: {error}");
            return;
        }
    };
    let mut writer = stream;
    for line in reader.lines() {
        let response = match line {
            Ok(line) => match serde_json::from_str::<MemoryRequest>(&line) {
                Ok(request) => dispatch(request, store),
                Err(error) => MemoryResponse::Error {
                    message: format!("invalid request: {error}"),
                },
            },
            Err(error) => MemoryResponse::Error {
                message: error.to_string(),
            },
        };
        let mut encoded = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
        encoded.push(b'\n');
        if writer.write_all(&encoded).is_err() {
            break;
        }
    }
}

fn dispatch(request: MemoryRequest, store: &MemoryStore) -> MemoryResponse {
    match request {
        MemoryRequest::Get { id } => match store.get(&id) {
            Ok(record) => MemoryResponse::Memory { record },
            Err(error) => MemoryResponse::Error {
                message: error.to_string(),
            },
        },
        MemoryRequest::Put { record } => match store.put(&record) {
            Ok(()) => MemoryResponse::Ok,
            Err(error) => MemoryResponse::Error {
                message: error.to_string(),
            },
        },
        MemoryRequest::List => match store.list() {
            Ok(records) => MemoryResponse::Memories { records },
            Err(error) => MemoryResponse::Error {
                message: error.to_string(),
            },
        },
        MemoryRequest::Revoke { id, revoked_at_ms } => match store.revoke(&id, revoked_at_ms) {
            Ok(()) => MemoryResponse::Ok,
            Err(error) => MemoryResponse::Error {
                message: error.to_string(),
            },
        },
    }
}

fn args() -> (PathBuf, PathBuf, bool) {
    let mut database = None;
    let mut socket = None;
    let mut initialize = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--database" => database = args.next().map(PathBuf::from),
            "--socket" => socket = args.next().map(PathBuf::from),
            "--initialize" => initialize = true,
            _ => {}
        }
    }
    let data = std::env::var_os("TACHYON_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::data_local_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("tachyon")
        });
    (
        database.unwrap_or_else(|| data.join("databases/memories.redb")),
        socket.unwrap_or_else(|| data.join("state/memory.sock")),
        initialize,
    )
}
