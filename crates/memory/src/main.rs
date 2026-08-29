#![forbid(unsafe_code)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use tachyon_memory::protocol::{MemoryRequest, MemoryResponse};
use tachyon_memory::MemoryStore;

fn main() -> std::io::Result<()> {
    let (root, socket) = args();
    let store = MemoryStore::open(root).map_err(std::io::Error::other)?;
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
        MemoryRequest::ReadTask { id } => match store.read_task(&id) {
            Ok(document) => MemoryResponse::Task { document },
            Err(error) => MemoryResponse::Error {
                message: error.to_string(),
            },
        },
        MemoryRequest::WriteTask { document } => match store.write_task(&document) {
            Ok(()) => MemoryResponse::Ok,
            Err(error) => MemoryResponse::Error {
                message: error.to_string(),
            },
        },
        MemoryRequest::ListTasks => match store.list_tasks() {
            Ok(documents) => MemoryResponse::Tasks { documents },
            Err(error) => MemoryResponse::Error {
                message: error.to_string(),
            },
        },
    }
}

fn args() -> (PathBuf, PathBuf) {
    let mut root = None;
    let mut socket = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => root = args.next().map(PathBuf::from),
            "--socket" => socket = args.next().map(PathBuf::from),
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
        root.unwrap_or_else(|| data.join("memory")),
        socket.unwrap_or_else(|| data.join("state/memory.sock")),
    )
}
