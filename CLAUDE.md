Automation for Driftwood Evaluation Center's client questionnaire workflow: sending pre-appointment questionnaires through third-party platforms, tracking completion, and sending reminders. Also generates billing/piecework reports. Config (DB credentials, service credentials, business rules) is fetched at runtime from a remote API (`api_url` in `config/local_config.yml`), not stored in this repo. This API is in ../winnonah.

## Code style
- No AI-isms (filler, hedging, "as an AI"), no em dashes, no overly clever/terse one-liners. Keep it simple and DRY.

## Commands
- `mise run check` to lint. Trust its output over your own assumptions about Python syntax validity.

## Architecture

### Entry-point scripts (repo root)
Each is a standalone Typer/argparse CLI, independently invoked (`qreceive` is also independently containerized/cron-scheduled):
- `qsend.py` - finds clients needing questionnaires (via the Google Sheets "prioritization list"), logs into each third-party platform, sends/generates the links.
- `qreceive.py` - checks in-progress questionnaires for completion, sends reminders via OpenPhone/Quo, syncs status back to the prioritization list and DB, emails admins a summary.
- `piecework.py` - computes evaluator billing from completed reports and the punch list, writes Excel to `piecework_output/`, uploads to Drive.
- `records-request.py` - automates downloading/requesting external records.
- `log-server.py` - plain TCP socket server; other scripts' loggers stream to it (`NetworkSink` in `utils/misc.py`), landing in per-app files under `logs/remote_<app>.log` on the remote server.

### Config and logging
- Real config (DB connection, per-service credentials, business rules like questionnaire batteries) comes from the remote API at startup and is validated against `FullConfig` - don't hardcode business rules that likely belong there.
- Each script logs to stdout, a local rotating file in `logs/`, and (for `qsend`/`piecework`) to `NetworkSink`. Use `NOTICE` for client-state skips, `ERROR` for real program/data issues.

## Restrictions
- Never read git-ignored files (via any tool, including grep/rg/cat) - treat them as off-limits.
- Never query or output sensitive data (PII, credentials, patient records) from the DB or API, even for debugging.
