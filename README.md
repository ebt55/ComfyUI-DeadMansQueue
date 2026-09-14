# ComfyUI — Dead Man's Queue

**Your queue survives the power cut.** Queue 20 jobs, lose power at job 3, relaunch ComfyUI — the remaining 17 are already running.

A dead man's switch fires when you can't. Same idea: ComfyUI keeps its prompt queue entirely in RAM, so when the power goes, every job you had waiting goes with it. This mirrors the queue to a crash-safe database and pushes the survivors back in on the next start. No clicks, no export/import dance, nothing to remember to do beforehand.

```
[DeadMansQueue] found 17 unfinished job(s) from the last session (1 was mid-render)
[DeadMansQueue] restored 17 job(s) - the queue is running again
```

## Install

```bash
cd ComfyUI/custom_nodes
git clone https://github.com/ebt55/ComfyUI-DeadMansQueue
```

Restart ComfyUI. That's the whole install.

Or in **ComfyUI-Manager** → *Install via Git URL* → paste the repo URL.

**No dependencies.** No `pip install`, no models, no settings, no nodes to wire into a workflow. It uses only Python's standard library, so it cannot break a portable install the way extensions that drag in their own package versions do. It is invisible until the day it saves you.

You should see this on startup:

```
[DeadMansQueue] armed - queue mirrored to .../user/deadmansqueue.sqlite3
```

## What it recovers

| | |
|---|---|
| Jobs still waiting in the queue | ✅ re-queued |
| The job that was mid-render | ✅ re-queued from the start |
| Original queue order and priority | ✅ preserved |
| Jobs you deleted or cleared yourself | ✅ stay gone |
| Jobs that already finished | ✅ not re-run |

A render that was halfway through is restarted, not resumed — resuming mid-sampling would mean serialising latents and sampler state on every step. Re-running one image is the better trade.

## Why not just dump JSON

Because a power cut is not a crash.

A process crash lets the OS flush what you already wrote. A power cut can stop the drive **mid-write**, leaving a truncated or zero-byte file — so the obvious approach loses the whole queue in exactly the scenario it was built for.

This uses SQLite in WAL mode with `synchronous=FULL`, so every commit is fsync'd before it is acknowledged. Pull the plug one millisecond after clicking Run and that job is already durable.

## Design

- **Nothing in the UI changes.** The native queue panel keeps working normally. This only instruments the queue object and ships no frontend code, so it doesn't fight other extensions that also touch the queue — rgthree, easy-use and friends are unaffected.
- **Credentials never reach disk.** A queue item's 6th field carries `auth_token_comfy_org` / `api_key_comfy_org`. It is stripped before writing, mirroring the `remove_sensitive` that ComfyUI itself applies before saving to history. Jobs using API nodes need re-authenticating after a recovery — the correct trade for not keeping tokens in a database.
- **A fallback that outlives the format.** On every recovery, each job's complete editor graph is also written out as a plain workflow `.json` under `user/deadmansqueue/recovered_<timestamp>/`. Queued items carry the full graph — the same payload ComfyUI embeds in saved PNGs — not just the flattened API prompt. So if a future ComfyUI or custom-node update ever makes a stored prompt unreplayable, you still have openable workflow files instead of nothing.
- **It cannot break your install.** Every database operation is individually guarded, and setup is wrapped so a persistence failure logs and disables itself rather than stopping ComfyUI from starting.
- **It stays small.** Rows are deleted as jobs complete, so the database only ever tracks what is actually outstanding.

## Checking on it

```
GET /deadmansqueue/status
```

```json
{
  "enabled": true,
  "database": ".../user/deadmansqueue.sqlite3",
  "persisted_pending": 17,
  "persisted_running": 1,
  "last_restore": { "restored": 17, "interrupted": 1, "graph_dir": "..." }
}
```

## Turning it off

Set `COMFY_DEADMANSQUEUE_DISABLE=1` before launching, or just delete the folder. The database is a single file in `user/` and is safe to remove at any time.

## Tested against

ComfyUI 0.27.0 / frontend 1.45.20, Windows portable build, alongside 17 other custom node packs.

Verified by queuing 40 jobs through the real `/prompt` endpoint and hard-killing the process mid-queue (`taskkill /F` — no flush, no cleanup), then restarting: 39 unfinished jobs restored in original order, credentials confirmed absent from the database and its write-ahead log by byte scan, and the database self-cleaned to zero rows once the backlog drained.

## Also worth doing

This recovers from a power cut; it doesn't prevent one. A line-interactive UPS is what stops the loss happening — and it protects your drives from the aborted writes that corrupt databases in the first place. The two solve different halves of the problem.

## License

MIT
