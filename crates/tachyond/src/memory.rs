use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tachyon_memory::protocol::{MemoryRequest, MemoryResponse};
use tachyon_memory::TaskDocument;

#[derive(Clone, Debug)]
pub struct MemoryClient {
    path: PathBuf,
}

impl MemoryClient {
    pub fn connect(path: impl Into<PathBuf>, wait: Duration) -> std::io::Result<Self> {
        let path = path.into();
        let deadline = Instant::now() + wait;
        loop {
            match UnixStream::connect(&path) {
                Ok(_) => return Ok(Self { path }),
                Err(error) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25));
                    let _ = error;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn list_tasks(&self) -> std::io::Result<Vec<TaskDocument>> {
        match self.request(MemoryRequest::ListTasks)? {
            MemoryResponse::Tasks { documents } => Ok(documents),
            MemoryResponse::Error { message } => Err(std::io::Error::other(message)),
            response => Err(std::io::Error::other(format!(
                "unexpected memory response: {response:?}"
            ))),
        }
    }

    pub fn write_task(&self, document: &TaskDocument) -> std::io::Result<()> {
        match self.request(MemoryRequest::WriteTask {
            document: document.clone(),
        })? {
            MemoryResponse::Ok => Ok(()),
            MemoryResponse::Error { message } => Err(std::io::Error::other(message)),
            response => Err(std::io::Error::other(format!(
                "unexpected memory response: {response:?}"
            ))),
        }
    }

    fn request(&self, request: MemoryRequest) -> std::io::Result<MemoryResponse> {
        let mut stream = UnixStream::connect(&self.path)?;
        let mut encoded = serde_json::to_vec(&request).map_err(std::io::Error::other)?;
        encoded.push(b'\n');
        stream.write_all(&encoded)?;
        stream.flush()?;
        let mut response = String::new();
        BufReader::new(stream).read_line(&mut response)?;
        if response.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "memory closed connection",
            ));
        }
        serde_json::from_str(response.trim()).map_err(std::io::Error::other)
    }
}
