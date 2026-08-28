# SolarIQ — Integration Status & Continuation Notes

**Last updated:** 2026-08-28
**Author of this note:** integration check performed with Claude Code, recorded here for whoever picks this up next.

This file tracks where the three-member merge stands, what has actually been
verified, and exactly what is left before Checkpoint D (Assessment Ready) can
be called done. See `SolarIQ_Master_Project_Specification.md` section 31 for
the full Definition of Done this is checked against.

---

## 1. Where things stand

All three member branches are now merged into `main` and pushed to `origin/main`:

```text
main  (b68e103)
  = member-1/platform-ingestion  (simulators/, kafka/, observability/, scripts/)
  + member-2/data-processing     (processing/, orchestration/, storage/)
  + member-3/serving-ui          (api/, dashboard/, reports/, tests/integration, tests/e2e)
```

Merge history:
- PR #3 already merged member-1 + member-2 into `main` before this check.
- member-3's branch (`origin/member-3/serving-ui`) had 52 commits that were
  **not visible locally until a fresh `git fetch --prune`** — if you're
  picking this up on a different clone, fetch first before assuming a branch
  is empty.
- The member-3 merge was done as a fast-forward merge commit (`b68e103`) after
  a dry-run merge (`--no-commit --no-ff`) confirmed **zero conflicts** across
  `docker-compose.yml`, `.env.example`, and `README.md` — the three shared
  files every member touches.

## 2. What has been verified so far

- Read all 4 governing docs (master spec + 3 member playbooks) and the
  existing handoff docs (`docs/architecture.md`, `docs/data-contracts.md`,
  `docs/member-1-handoff.md`, `docs/member-2-handoff.md`) before touching
  anything.
- Confirmed member-3's implementation actually matches its playbook: FastAPI
  app (config/db/repositories/routers/models), all 4 required dashboard pages
  (Portfolio, Plant, Alerts, Daily Report), API tests, dashboard tests, one
  integration test file, one e2e smoke test, and `docs/demo-runbook.md`.
- Spot-checked several of member-3's files against member-2's documented
  contracts — all correct:
  - Portfolio performance is energy-weighted (`sum(actual)/sum(expected)`),
    never an average of plant percentages.
  - NULL semantics respected (no fabricated zeros for stale/no-data states).
  - Freshness (`LIVE`/`STALE`/`NO_DATA`) computed by the API reader against
    `STALE_DATA_SECONDS`, not stored — matches member-2's handoff exactly.
  - 404 returned for a daily report that hasn't been reconciled yet, not a
    fabricated empty report.
- Grepped `api/` and `dashboard/src/` for `TODO`/`FIXME`/hardcoded/mock
  values — none found (master spec rules 16–17).
- Ran the full existing unit-test suite against the merged tree:
  `pytest tests/simulators tests/storage tests/batch tests/processing -m "not integration and not spark"`
  → **547 passed, 8 skipped, 107 deselected.** No regressions from the merge.
- Found two harmless **untracked** stray directories in the working tree,
  `processing;C` and `storage;C` (empty, not tracked by git — leftover from
  some earlier mangled shell command). Left alone; safe to `rm -rf` them
  locally whenever convenient, they are not part of the repo.

## 3. What is NOT yet verified — continue here

Blocked purely on this machine's slow/Avast-intercepted network, not on any
known code problem:

1. **`api/tests/*` unit tests** — need `fastapi`, `httpx`, `uvicorn` installed
   into `.venv`. Installs were kept timing out
   (`ReadTimeoutError: files.pythonhosted.org`) even with `--cert` pointed at
   the Avast bundle and a 600s/10-retry timeout. See
   `[[avast-breaks-pip-ssl]]`-style notes: this is a known machine quirk, not
   a project issue. Resume with:
   ```bash
   .venv/Scripts/python.exe -m pip install --cert "C:\ProgramData\Avast Software\Avast\wscert.pem" --timeout 600 --retries 10 -r api/requirements-dev.txt
   .venv/Scripts/python.exe -m pytest api/tests -m "not integration" -q
   ```

2. **Dashboard `tsc` + `vitest`** — `npm ci` in `dashboard/` was still
   populating `node_modules` (236+ packages in) when paused. Resume with:
   ```bash
   cd dashboard && npm ci
   npx tsc --noEmit
   npx vitest run
   ```

3. **Integration tests requiring live infrastructure** — not attempted this
   session (would need the full Docker Compose stack: Kafka, Postgres, MinIO,
   Airflow). These are the tests marked `@pytest.mark.integration` across
   `tests/`, plus `tests/integration/test_api_integration.py` and
   `tests/e2e/smoke_test.py`. Needs:
   ```bash
   ./scripts/bootstrap.sh
   # then, per each subsystem's own instructions in its handoff doc
   ```

4. **Full Docker Compose validation** — `docker compose config` /
   `docker compose up` from a clean environment was not run this session.
   Master spec section 31 "Quality" requires this before calling the project
   done.

5. **Full master-spec Definition of Done pass (section 31)** — only spot
   checked, not walked item by item. Worth a dedicated pass once 1–4 above are
   green, since a few items need a running stack to confirm (e.g. "Airflow
   runs daily reconciliation", "alerts are generated", "fresh-clone smoke test
   succeeds").

6. **A full read of member-3's remaining files not yet opened this session**:
   `api/app/main.py`, `api/app/repositories/plants.py`, `api/app/routers/portfolio.py`,
   `api/app/models/*.py`, and the full dashboard component tree beyond
   `PortfolioPage.tsx` and `types.ts`. What was read looked correct and
   consistent; the rest wasn't read line-by-line, just confirmed to exist and
   pass its own tests where those could run.

## 4. Suggested next session's checklist

- [ ] Finish installing `api/requirements-dev.txt`, run `api/tests`.
- [ ] Finish `dashboard` `npm ci`, run `tsc --noEmit` and `vitest run`.
- [ ] `docker compose config` (validate compose file syntactically).
- [ ] `docker compose up -d` from a clean state, confirm all services healthy.
- [ ] Run `scripts/bootstrap.sh` → `scripts/demo_start.sh`, confirm the
      live path (simulator → Kafka → Spark → Postgres → API → dashboard).
- [ ] Trigger the Airflow DAG once, confirm `daily_plant_summary` populates
      and `/api/v1/reports/daily` stops 404ing.
- [ ] Run `tests/e2e/smoke_test.py --full`.
- [ ] Walk master spec section 31 Definition of Done top to bottom, check
      off each item against actual observed behavior (not assumption).
- [ ] Rehearse the demo timeline in `docs/demo-runbook.md` once, end to end.

## 5. Known environment notes for whoever continues this

- This machine's pip/npm traffic is intercepted by Avast, which breaks fresh
  TLS verification and can make large package downloads slow/timeout. Use
  `--cert "C:\ProgramData\Avast Software\Avast\wscert.pem"` for pip; npm has
  not needed special handling but has been slow.
- Windows: run the project's `.sh` scripts from Git Bash or WSL, not
  PowerShell (per `docs/member-1-handoff.md` section 9).
- There is a separate git worktree for member-3 at
  `C:/Users/mdska/Documents/SolarIQ-member3` and one for member-1 at
  `C:/Users/mdska/Documents/SolarIQ-member1` — if a branch looks stale or
  "can't force update", check `git worktree list` before assuming the branch
  itself has no work.
