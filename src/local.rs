//! Local shell session worker.
//!
//! Spawns a local shell (cmd.exe / PowerShell / bash / zsh / sh) attached to a
//! pseudo-terminal (PTY) so that interactive programs (vim, htop, etc.) work
//! correctly with full ANSI/VT100 support.
//!
//! Uses the [`portable_pty`] crate for cross-platform PTY abstraction:
//! - **Windows**: ConPTY (Windows 10 1809+)
//! - **Unix**: POSIX PTY (posix_openpt)
//!
//! The public surface mirrors [`crate::ssh::spawn_session`] and
//! [`crate::serial::spawn_serial_session`] so the rest of the UI pipeline
//! (terminal output, key input, tab lifecycle) is reused unchanged.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use portable_pty::{native_pty_system, ChildKiller, CommandBuilder, PtySize, PtySystem};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::config::Session;
use crate::i18n::t;
use crate::ssh::{SessionCommand, SessionEvent, SessionHandle};

/// Spawn a local shell session.
///
/// The signature mirrors [`crate::ssh::spawn_session`] (minus the `Session`
/// fields that are irrelevant for a local shell).
pub fn spawn_local_session(
    runtime: &tokio::runtime::Handle,
    tab_id: String,
    _session: Session,
    initial_cols: u32,
    initial_rows: u32,
) -> (SessionHandle, UnboundedReceiver<SessionEvent>) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (evt_tx, evt_rx) = mpsc::unbounded_channel::<SessionEvent>();

    let evt_for_task = evt_tx.clone();
    let join = runtime.spawn(async move {
        if let Err(err) = run_local(cmd_rx, evt_for_task.clone(), initial_cols, initial_rows).await
        {
            let _ = evt_for_task.send(SessionEvent::Closed(format!("{err:#}")));
        }
    });

    (
        SessionHandle {
            tab_id,
            commands: cmd_tx,
            join,
        },
        evt_rx,
    )
}

/// Determine the shell executable and optional initial arguments.
///
/// Resolution order:
/// 1. `SHELL` environment variable (POSIX convention, works on Windows too)
/// 2. `ComSpec` environment variable (Windows convention)
/// 3. Platform default: `/bin/sh` on Unix, `cmd.exe` on Windows
fn resolve_shell() -> (String, Vec<String>) {
    // Check SHELL first (common on Unix, also set by some Windows shells).
    if let Ok(shell) = std::env::var("SHELL") {
        if !shell.is_empty() {
            return (shell, Vec::new());
        }
    }

    // Windows fallback: ComSpec (usually C:\Windows\System32\cmd.exe).
    #[cfg(windows)]
    {
        if let Ok(comspec) = std::env::var("ComSpec") {
            if !comspec.is_empty() {
                return (comspec, Vec::new());
            }
        }
        ("cmd.exe".to_string(), Vec::new())
    }

    #[cfg(not(windows))]
    {
        ("/bin/sh".to_string(), Vec::new())
    }
}

/// The async worker body: create a PTY, spawn a shell, and pump I/O.
async fn run_local(
    mut commands: mpsc::UnboundedReceiver<SessionCommand>,
    events: UnboundedSender<SessionEvent>,
    initial_cols: u32,
    initial_rows: u32,
) -> anyhow::Result<()> {
    let _ = events.send(SessionEvent::Status(t(
        "正在启动本地终端...",
        "Starting local terminal...",
    )));

    // Create a PTY pair.
    let pty_system = native_pty_system();
    let pty_size = PtySize {
        rows: initial_rows.max(1) as u16,
        cols: initial_cols.max(1) as u16,
        pixel_width: 0,
        pixel_height: 0,
    };
    let pair = pty_system
        .openpty(pty_size)
        .map_err(|e| anyhow::anyhow!("{}: {e}", t("创建 PTY 失败", "failed to create PTY")))?;
    let master = pair.master;
    let slave = pair.slave;

    // Build the shell command.
    let (shell_path, shell_args) = resolve_shell();
    let mut cmd = CommandBuilder::new(&shell_path);
    for arg in &shell_args {
        cmd.arg(arg);
    }

    // Set environment variables for proper terminal behavior.
    cmd.env("TERM", "xterm-256color");
    // On Windows, set ConPTY-related env vars for proper ANSI support.
    #[cfg(windows)]
    {
        cmd.env("ConEmuANSI", "ON");
    }

    // Spawn the child process attached to the slave end of the PTY.
    let child = slave
        .spawn_command(cmd)
        .map_err(|e| anyhow::anyhow!("{}: {e}", t("启动 shell 失败", "failed to spawn shell")))?;

    // We no longer need the slave end — the child owns it.
    drop(slave);

    let _ = events.send(SessionEvent::Connected);
    let _ = events.send(SessionEvent::Status(format!(
        "{} {}",
        t("已连接", "Connected"),
        shell_path,
    )));

    // Take the reader and writer from the master PTY.
    let reader = master
        .try_clone_reader()
        .map_err(|e| anyhow::anyhow!("{}: {e}", t("PTY 读取端克隆失败", "failed to clone PTY reader")))?;
    let writer = master
        .take_writer()
        .map_err(|e| anyhow::anyhow!("{}: {e}", t("PTY 写入端获取失败", "failed to take PTY writer")))?;

    // Wrap in Arc<Mutex> for sharing with the command pump (via spawn_blocking).
    let writer = Arc::new(std::sync::Mutex::new(writer));
    // Child is only accessed from this task, no Arc needed.
    let child = std::sync::Mutex::new(child);

    // --- Reader thread (dedicated OS thread, same pattern as serial.rs) ---
    let running = Arc::new(AtomicBool::new(true));
    let reader_running = running.clone();
    let reader_events = events.clone();
    let reader_handle = std::thread::spawn(move || {
        let mut reader = reader;
        let mut buf = [0u8; 8192];
        use std::io::Read;
        while reader_running.load(Ordering::Relaxed) {
            match reader.read(&mut buf) {
                Ok(0) => {
                    // EOF — the shell exited.
                    let _ = reader_events.send(SessionEvent::Closed(t(
                        "本地终端已退出",
                        "local terminal exited",
                    )));
                    break;
                }
                Ok(n) => {
                    let text = String::from_utf8_lossy(&buf[..n]).into_owned();
                    if reader_events.send(SessionEvent::Output(text)).is_err() {
                        break;
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::TimedOut
                        || e.kind() == std::io::ErrorKind::Interrupted =>
                {
                    continue;
                }
                Err(e) => {
                    let _ = reader_events.send(SessionEvent::Closed(format!(
                        "{}: {e}",
                        t("PTY 读取错误", "PTY read error")
                    )));
                    break;
                }
            }
        }
    });

    // --- Command pump (tokio select loop) ---
    while let Some(cmd) = commands.recv().await {
        match cmd {
            SessionCommand::RawInput(bytes) => {
                let w = writer.clone();
                let res = tokio::task::spawn_blocking(move || {
                    use std::io::Write;
                    let mut guard = w.lock().unwrap();
                    guard.write_all(&bytes).and_then(|_| guard.flush())
                })
                .await;
                if let Ok(Err(e)) = res {
                    let _ = events.send(SessionEvent::Closed(format!(
                        "{}: {e}",
                        t("PTY 写入失败", "PTY write failed")
                    )));
                    break;
                }
            }
            SessionCommand::Resize(cols, rows) => {
                let new_size = PtySize {
                    rows: rows.max(1) as u16,
                    cols: cols.max(1) as u16,
                    pixel_width: 0,
                    pixel_height: 0,
                };
                if let Err(e) = master.resize(new_size) {
                    tracing::warn!("PTY resize failed: {e}");
                }
            }
            SessionCommand::Close => break,
        }
    }

    // Cleanup: stop the reader thread, kill the child, close the PTY.
    running.store(false, Ordering::Relaxed);

    // Kill the child process so the reader thread's read() returns EOF.
    if let Ok(mut child_guard) = child.lock() {
        let _ = child_guard.kill();
    }

    let _ = reader_handle.join();

    // If we didn't already send a Closed event (e.g. user clicked close tab),
    // send one now.
    let _ = events.send(SessionEvent::Closed(t(
        "本地终端已关闭",
        "local terminal closed",
    )));

    Ok(())
}
