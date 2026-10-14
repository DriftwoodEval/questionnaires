Automation for Driftwood Evaluation Center's client questionnaire workflow: sending pre-appointment questionnaires through third-party platforms, tracking completion, and sending reminders. Also generates billing/piecework reports. Config (DB credentials, service credentials, business rules) is fetched at runtime from a remote API (`api_url` in `config/local_config.yml`), not stored in this repo. This API is in ../winnonah.

## Code style
- No AI-isms (filler, hedging, "as an AI"), no em dashes, no overly clever/terse one-liners. Keep it simple and DRY.

## Commands
- `mise run check` to lint. Trust its output over your own assumptions about Python syntax validity.

## Testing
- Prefer `pytest.mark.parametrize` over near-duplicate test methods that only vary in input/expected values. Only keep separate test methods when the setup or assertions genuinely differ.
- When changing a function's signature, return shape, or behavior, grep its test files for every call site and check whether existing tests still hold, not just whether they still pass. If a change will break a test, say so and either update the test yourself or flag it with the specific test name and why, don't leave it for CI to discover. Don't delete or weaken a test to make it pass; fix the test's expectations to match the new intended behavior, or fix the code if the test caught a real regression.
- Run `mise run check` after editing Python, ahead of pytest: a ruff failure (unused import, bad type annotation) is often the faster signal, and trust its output over your own assumptions about Python syntax validity.
- This repo has no live browser or end-to-end tests to stand up; verify changes via pytest and `mise run check` only.

## Architecture

### Entry-point scripts (repo root)
Each is a standalone Typer/argparse CLI, independently invoked (`qreceive` is also independently containerized/cron-scheduled):
- `qsend.py` - finds clients needing questionnaires (via the Google Sheets "prioritization list"), logs into each third-party platform, sends/generates the links.
- `qreceive.py` - checks in-progress questionnaires for completion, sends reminders via Quo, syncs status back to the prioritization list and DB, emails admins a summary.
- `piecework.py` - computes evaluator billing from completed reports and the punch list, writes Excel to `piecework_output/`, uploads to Drive.
- `records-request.py` - automates downloading/requesting external records.
- `log-server.py` - plain TCP socket server; other scripts' loggers stream to it (`NetworkSink` in `utils/misc.py`), landing in per-app files under `logs/remote_<app>.log` on the remote server.

### Config and logging
- Real config (DB connection, per-service credentials, business rules like questionnaire batteries) comes from the remote API at startup and is validated against `FullConfig` - don't hardcode business rules that likely belong there.
- Each script logs to stdout, a local rotating file in `logs/`, and (for `qsend`/`piecework`) to `NetworkSink`. Use `NOTICE` for client-state skips, `ERROR` for real program/data issues.

### Timezone handling
This repo shares the `../winnonah` MySQL DB. `emr_appointment.startTime`/`endTime`, `sessionStartedAt`, and similar timestamp columns are genuine UTC instants; date-only columns (`sent`, `failedDate`, `dob`, etc.) are business-local calendar dates with no time component. Never combine a date-only value with midnight and compare it against a UTC instant without converting: use `driftwood_common.timezone`'s `business_to_utc`/`business_date_to_utc` (naive business-local → UTC), `utc_to_business` (UTC → naive business-local), and `now_utc`/`now_business` for "now" in either domain. These come from the shared `driftwood-common` package (`python/common` in `../winnonah`), installed as a git dependency pinned by `uv.lock`; to pick up changes there, run `uv lock --upgrade-package driftwood-common`. The practice's timezone is `config.business_timezone` (from `load_config()`) where a `Config` is in scope, otherwise `driftwood_common.constants.BUSINESS_TIMEZONE`, which is also what the helpers default to. Code needed by both repos belongs in `driftwood-common`, not in a copy here. Never hardcode a timezone string or rely on the container's ambient `TZ` env var for correctness.

## Restrictions
- Never read git-ignored files (via any tool, including grep/rg/cat) - treat them as off-limits.
- Never query or output sensitive data (PII, credentials, patient records) from the DB or API, even for debugging.
