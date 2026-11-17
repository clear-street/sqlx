use crate::net::Socket;

use std::io::{self, Read, Write};
use std::task::{Context, Poll};

use futures_util::future;

pub struct StdSocket<S> {
    pub socket: S,
    wants_read: bool,
    wants_write: bool,
}

impl<S: Socket> StdSocket<S> {
    pub fn new(socket: S) -> Self {
        Self {
            socket,
            wants_read: false,
            wants_write: false,
        }
    }

    /// Returns `Ready` if a previously blocked read _or_ write may now proceed.
    ///
    /// If both a read and a write were attempted, to avoid deadlocks this returns `Ready`
    /// when _either_ direction is ready, not necessarily both.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Return `Ready` without waiting if the caller hasn't tried to do I/O in either direction.
        let mut ready = !(self.wants_read || self.wants_write);

        if self.wants_write && self.socket.poll_write_ready(cx)?.is_ready() {
            self.wants_write = false;
            ready |= true;
        }

        if self.wants_read && self.socket.poll_read_ready(cx)?.is_ready() {
            self.wants_read = false;
            ready |= true;
        }

        if ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    /// Returns successfully if a previously blocked read _or_ write may now proceed.
    ///
    /// If both a read and a write were attempted, to avoid deadlocks this returns when _either_
    /// direction is ready, not necessarily both.
    pub async fn ready(&mut self) -> io::Result<()> {
        future::poll_fn(|cx| self.poll_ready(cx)).await
    }
}

impl<S: Socket> Read for StdSocket<S> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        self.wants_read = true;
        let read = self.socket.try_read(&mut buf)?;
        self.wants_read = false;

        Ok(read)
    }
}

impl<S: Socket> Write for StdSocket<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.wants_write = true;
        let written = self.socket.try_write(buf)?;
        self.wants_write = false;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        // NOTE: TCP sockets and unix sockets are both no-ops for flushes
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::ReadBuf;

    struct BackpressuredSocket {
        read_ready: bool,
        write_ready: bool,
        read_polled: bool,
        write_polled: bool,
    }

    impl Socket for BackpressuredSocket {
        fn try_read(&mut self, _buf: &mut dyn ReadBuf) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn try_write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn poll_read_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.read_polled = true;
            if self.read_ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn poll_write_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.write_polled = true;
            if self.write_ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn blocked_tls_io_resumes_when_either_direction_becomes_ready() {
        for (read_ready, write_ready) in [(false, true), (true, false), (true, true)] {
            let mut socket = StdSocket::new(BackpressuredSocket {
                read_ready,
                write_ready,
                read_polled: false,
                write_polled: false,
            });
            assert_eq!(
                Read::read(&mut socket, &mut [0]).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(
                Write::write(&mut socket, &[0]).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
            assert!(
                matches!(socket.poll_ready(&mut cx), Poll::Ready(Ok(()))),
                "TLS must resume when read_ready={read_ready}, write_ready={write_ready}"
            );
        }
    }

    #[test]
    fn blocked_tls_io_registers_both_readiness_interests() {
        let mut socket = StdSocket::new(BackpressuredSocket {
            read_ready: false,
            write_ready: false,
            read_polled: false,
            write_polled: false,
        });
        assert!(Read::read(&mut socket, &mut [0]).is_err());
        assert!(Write::write(&mut socket, &[0]).is_err());
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(socket.poll_ready(&mut cx).is_pending());
        assert!(socket.socket.read_polled);
        assert!(socket.socket.write_polled);
    }
}
