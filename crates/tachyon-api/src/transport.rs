#![forbid(unsafe_code)]

//! Newline-delimited JSON framing over a Unix domain socket.

use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

use crate::types::{ApiRequest, ApiResponse};

/// Write a request as a single JSON line to `writer`.
pub fn write_request<W: Write>(out: &mut W, req: &ApiRequest) -> io::Result<()> {
    let mut buf = serde_json::to_vec(req).map_err(io::Error::other)?;
    buf.push(b'\n');
    out.write_all(&buf)?;
    out.flush()
}

/// Write a response as a single JSON line to `writer`.
pub fn write_response<W: Write>(out: &mut W, res: &ApiResponse) -> io::Result<()> {
    let mut buf = serde_json::to_vec(res).map_err(io::Error::other)?;
    buf.push(b'\n');
    out.write_all(&buf)?;
    out.flush()
}

/// Read a request (one line) from `reader`.
pub fn read_request<R: BufRead>(input: &mut R) -> io::Result<ApiRequest> {
    let mut line = String::new();
    input.read_line(&mut line)?;
    if line.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection closed",
        ));
    }
    serde_json::from_str(line.trim()).map_err(io::Error::other)
}

/// Read a response (one line) from `reader`.
pub fn read_response<R: BufRead>(input: &mut R) -> io::Result<ApiResponse> {
    let mut line = String::new();
    input.read_line(&mut line)?;
    if line.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection closed",
        ));
    }
    serde_json::from_str(line.trim()).map_err(io::Error::other)
}

/// A convenience wrapper over a connected stream.
pub struct Connection {
    reader: BufReader<std::os::unix::net::UnixStream>,
    stream: std::os::unix::net::UnixStream,
}

impl Connection {
    pub fn connect<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let stream = std::os::unix::net::UnixStream::connect(path)?;
        let reader = BufReader::new(stream.try_clone()?);
        Ok(Self { stream, reader })
    }

    /// Send a request and read the synchronous response.
    pub fn exchange(&mut self, req: &ApiRequest) -> io::Result<ApiResponse> {
        write_request(&mut self.stream, req)?;
        read_response(&mut self.reader)
    }

    /// Send a request without reading a response (used to open a stream).
    pub fn send(&mut self, req: &ApiRequest) -> io::Result<()> {
        write_request(&mut self.stream, req)
    }

    /// Read the next response line (used on an open stream).
    pub fn recv(&mut self) -> io::Result<ApiResponse> {
        read_response(&mut self.reader)
    }

    /// Set a timeout on reads (e.g. for slow operations).
    pub fn set_read_timeout(&self, dur: Option<std::time::Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(dur)
    }

    /// Clone only for cancellation via `shutdown`; do not read/write this handle.
    pub fn shutdown_handle(&self) -> io::Result<std::os::unix::net::UnixStream> {
        self.stream.try_clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::Shutdown, os::unix::net::UnixStream, sync::mpsc, time::Duration};

    #[test]
    fn shutdown_handle_interrupts_a_partial_response_without_read_timeout() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut connection = Connection {
            reader: BufReader::new(stream.try_clone().unwrap()),
            stream,
        };
        let shutdown = connection.shutdown_handle().unwrap();
        peer.write_all(b"{\"event\":").unwrap();
        let (done, completed) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            done.send(connection.recv().is_err()).unwrap();
        });
        assert!(completed.recv_timeout(Duration::from_millis(30)).is_err());
        shutdown.shutdown(Shutdown::Both).unwrap();
        assert!(completed.recv_timeout(Duration::from_secs(2)).unwrap());
        reader.join().unwrap();
    }
}
