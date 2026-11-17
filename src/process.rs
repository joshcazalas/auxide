use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    time,
};

#[derive(Clone, Debug)]
pub struct CommandSpec {
    program: PathBuf,
    args: Vec<OsString>,
    timeout: Duration,
    max_output_bytes: usize,
}

impl CommandSpec {
    #[must_use]
    pub fn new(program: impl Into<PathBuf>, timeout: Duration, max_output_bytes: usize) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            timeout,
            max_output_bytes,
        }
    }

    #[must_use]
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push(arg.as_ref().to_owned());
        self
    }

    #[must_use]
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        self
    }

    /// Runs the command directly, without a shell, and captures bounded output.
    ///
    /// # Errors
    ///
    /// Returns an error when the process cannot be started or reaped, exceeds its time or output
    /// limit, or its output reader fails.
    pub async fn run(&self) -> Result<CommandOutput, ProcessError> {
        if self.max_output_bytes == 0 {
            return Err(ProcessError::InvalidOutputLimit);
        }

        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        #[cfg(unix)]
        command.process_group(0);

        let mut child = command.spawn().map_err(|source| ProcessError::Spawn {
            program: self.program.clone(),
            source,
        })?;
        #[cfg(unix)]
        let mut group = ProcessGroup(
            child
                .id()
                .and_then(|id| i32::try_from(id).ok())
                .and_then(rustix::process::Pid::from_raw),
        );
        let stdout = child
            .stdout
            .take()
            .ok_or(ProcessError::MissingPipe("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or(ProcessError::MissingPipe("stderr"))?;
        let collect = async {
            let (stdout, stderr) = tokio::try_join!(
                bounded_read(stdout, self.max_output_bytes),
                bounded_read(stderr, self.max_output_bytes)
            )?;
            // Keep the leader unreaped until both pipes close so its process
            // group ID remains reserved during cancellation.
            let status = child.wait().await.map_err(ProcessError::Wait)?;
            Ok::<_, ProcessError>((status, stdout, stderr))
        };
        let (status, stdout, stderr) =
            time::timeout(self.timeout, collect)
                .await
                .map_err(|_| ProcessError::Timeout {
                    program: self.program.clone(),
                    timeout: self.timeout,
                })??;
        #[cfg(unix)]
        {
            group.0 = None;
        }

        Ok(CommandOutput {
            status,
            stdout,
            stderr,
        })
    }

    #[must_use]
    pub fn program(&self) -> &Path {
        &self.program
    }
}

#[derive(Debug)]
pub struct CommandOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[cfg(unix)]
struct ProcessGroup(Option<rustix::process::Pid>);

#[cfg(unix)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if let Some(group) = self.0 {
            let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
        }
    }
}

async fn bounded_read<R: AsyncRead + Unpin>(
    reader: R,
    max_bytes: usize,
) -> Result<Vec<u8>, ProcessError> {
    let limit = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
    reader
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(ProcessError::Read)?;
    if bytes.len() > max_bytes {
        return Err(ProcessError::OutputLimit { max_bytes });
    }
    Ok(bytes)
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("process output limit must be greater than zero")]
    InvalidOutputLimit,
    #[error("failed to spawn {program}: {source}")]
    Spawn {
        program: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("spawned process did not expose its {0} pipe")]
    MissingPipe(&'static str),
    #[error("process timed out after {timeout:?}: {program}")]
    Timeout { program: PathBuf, timeout: Duration },
    #[error("failed while waiting for process: {0}")]
    Wait(#[source] std::io::Error),
    #[error("failed while reading process output: {0}")]
    Read(#[source] std::io::Error),
    #[error("process emitted more than the configured {max_bytes} bytes")]
    OutputLimit { max_bytes: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn captures_bounded_output_without_a_shell() {
        let result = CommandSpec::new("printf", Duration::from_secs(2), 16)
            .arg("hello")
            .run()
            .await
            .unwrap();
        assert!(result.status.success());
        assert_eq!(result.stdout, b"hello");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_deadline_includes_readers_inherited_by_helpers() {
        let command =
            CommandSpec::new("sh", Duration::from_millis(100), 16).args(["-c", "sleep 4 &"]);
        let result = time::timeout(Duration::from_secs(2), command.run())
            .await
            .unwrap();
        assert!(matches!(result, Err(ProcessError::Timeout { .. })));
    }

    #[tokio::test]
    async fn enforces_output_limit() {
        let result = CommandSpec::new("printf", Duration::from_secs(2), 4)
            .arg("hello")
            .run()
            .await;
        assert!(matches!(result, Err(ProcessError::OutputLimit { .. })));
    }
}
