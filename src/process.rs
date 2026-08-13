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
    task::JoinHandle,
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

        let mut child = command.spawn().map_err(|source| ProcessError::Spawn {
            program: self.program.clone(),
            source,
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ProcessError::MissingPipe("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or(ProcessError::MissingPipe("stderr"))?;
        let stdout_task = spawn_bounded_read(stdout, self.max_output_bytes);
        let stderr_task = spawn_bounded_read(stderr, self.max_output_bytes);

        let Ok(status) = time::timeout(self.timeout, child.wait()).await else {
            let _ = child.kill().await;
            let _ = join_reader(stdout_task).await;
            let _ = join_reader(stderr_task).await;
            return Err(ProcessError::Timeout {
                program: self.program.clone(),
                timeout: self.timeout,
            });
        };
        let status = status.map_err(ProcessError::Wait)?;

        let stdout = join_reader(stdout_task).await?;
        let stderr = join_reader(stderr_task).await?;

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

fn spawn_bounded_read<R>(reader: R, max_bytes: usize) -> JoinHandle<Result<Vec<u8>, ProcessError>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
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
    })
}

async fn join_reader(
    task: JoinHandle<Result<Vec<u8>, ProcessError>>,
) -> Result<Vec<u8>, ProcessError> {
    task.await.map_err(ProcessError::ReaderTask)?
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
    #[error("process output reader task failed: {0}")]
    ReaderTask(#[source] tokio::task::JoinError),
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

    #[tokio::test]
    async fn enforces_output_limit() {
        let result = CommandSpec::new("printf", Duration::from_secs(2), 4)
            .arg("hello")
            .run()
            .await;
        assert!(matches!(result, Err(ProcessError::OutputLimit { .. })));
    }
}
