# ThetaData nightly run

`curator theta nightly` keeps the `theta` schema current after the cash session. It loads Options Standard SPXW only. It does not load index, Cboe, or FRED data.

The command plans with `curator theta plan` and loads with the same aiomultiprocess pool and the same account-wide cap of 4 in-flight requests as `curator theta backfill`. It holds the machine lock (`THETADATA_LOCK_PATH`, default `/tmp/curator-theta.lock`) for the whole run, so a manual backfill on the same machine cannot start a second cap.

## What one run does

The clock is America/New_York, including DST.

- On a weekend or NYSE holiday the command prints a skip line and exits 0. It does not wait until the next session.
- Before 17:15 ET on a trading day it exits 0 with a message. That is the default. `--wait` sleeps until 17:15 and then runs. Half days use 17:15, not the early cash close.
- The window is the last `--lookback` NYSE sessions ending today. The default is 10. `THETADATA_NIGHTLY_LOOKBACK` sets the same value. The window never starts before 2022-05-16.
- A session that is already done or empty for every requested dataset is not requested.
- A session that was never planned, or that still has pending, running, or failed jobs, is planned and loaded. A finished day between two gaps does not hide those gaps.
- Running rows in the window are returned to pending first. The nightly process holds the machine lock, so those rows have no live worker.
- A second run of a finished window logs `requests=0` and `new_rows=0`.
- `jobs_failed` greater than zero exits 1 so systemd marks the run failed.
- The log is counts only. It does not print row values or the database URL.
- Sessions older than the lookback stay for `curator theta backfill`.

```text
skipped=0
sessions_due=1
requests=0
new_rows=0
jobs_done=0
jobs_empty=0
jobs_failed=0
failure_reasons=
```

## systemd user timer

On the WSL host, Theta Terminal v3 is the systemd user service `~/.config/systemd/user/theta-terminal.service`. It listens on `http://127.0.0.1:25503/v3`. It is started by hand until you enable it. The nightly service sets `After=theta-terminal.service` and `Wants=theta-terminal.service`, so systemd orders the load after the terminal and starts the terminal when the timer fires.

User services stop at logout unless linger is on. Enable linger, then enable the terminal and the nightly timer:

```bash
loginctl enable-linger "$USER"
systemctl --user status theta-terminal.service
systemctl --user enable --now theta-terminal.service
```

`systemctl --user status theta-terminal.service` is the check that the terminal is running. The listener should be `127.0.0.1:25503`. `Wants=theta-terminal.service` asks systemd to start that unit; it does not embed a URL or a credential.

The timer fires at 17:30 America/New_York, Monday through Friday, with `Persistent=true`. If the machine was off at 17:30, systemd runs the service once when it comes back. A Saturday fire from that catch-up exits 0 because Saturday is not a session. Monday's run loads the missed sessions that are still inside the lookback. The `America/New_York` suffix needs systemd 247 or newer, which keeps the fire time stable when the machine timezone is not Eastern. Confirm the next fires with:

```bash
systemd-analyze calendar 'Mon..Fri *-*-* 17:30:00 America/New_York' --iterations=3
```

Install the units from this repo. Edit `WorkingDirectory`, `EnvironmentFile`, and `ExecStart` in the service when the checkout is not `~/curator`. Enable the timer, not the service. Enabling the service would start a load at login.

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/user/curator-theta-nightly.service ~/.config/systemd/user/
cp deploy/systemd/user/curator-theta-nightly.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now curator-theta-nightly.timer
systemctl --user list-timers curator-theta-nightly.timer
```

The service reads `EnvironmentFile=%h/curator/.env`. systemd refuses to start when that file is missing. Put `THETADATA_DATABASE_URL` in the file. Do not write the URL in the unit. The repository gitignores `.env`. Optional keys are `THETADATA_API_BASE_URL`, `THETADATA_API_CONNECT_TIMEOUT` (default 10 seconds), `THETADATA_API_READ_TIMEOUT` (default 120 seconds), `THETADATA_API_TIMEOUT` (legacy read timeout, used when the read timeout is not set), `THETADATA_LOCK_PATH`, `THETADATA_WORKER_COUNT`, and `THETADATA_NIGHTLY_LOOKBACK`. The API base defaults to the local terminal. A failed job is logged with its root, date, endpoint, and reason. The counts include `failure_reasons` next to `jobs_failed`.

```bash
install -m 600 /dev/null ~/curator/.env
```

Create the virtualenv with `uv sync` so `~/curator/.venv/bin/curator` exists. Apply the theta schema once before the first persisted run:

```bash
uv run curator theta db upgrade
```

The unit runs `curator theta nightly` with no `--wait`, because 17:30 is after the gate. `TimeoutStartSec=infinity` lets a lookback finish. The journal is the counts log:

```bash
journalctl --user -u curator-theta-nightly.service -n 50
systemctl --user status curator-theta-nightly.service
```
