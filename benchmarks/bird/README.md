# BIRD Mini-Dev (PostgreSQL) benchmark

Reproduces the numbers in [`../../docs/BIRD_REPORT.md`](../../docs/BIRD_REPORT.md).

| Folder / file | What it is |
|---|---|
| `bird_bench.py` | The runner. `run` is resumable; `report` rebuilds `summary.json` and `results.csv`. |
| `run_official_eval.sh` | Fetches BIRD's own scoring scripts and runs EX, Soft-F1 and R-VES on a predictions file. |
| `predictions/predict_mini_dev_PostgreSQL.json` | NatQuery's answers in BIRD's submission format. |
| `results/results.jsonl` | One record per question: generated SQL, gold SQL, tokens, LLM calls, timings, outcome. |
| `results/results.csv`, `results/summary.json` | Flat table and every aggregate in the report. |
| `official_logs/` | Output of BIRD's own scripts for this run. |

## Reproduce

1. Get the BIRD Mini-Dev PostgreSQL files from the [BIRD mini_dev repo](https://github.com/bird-bench/mini_dev) and restore the SQL dump into a database named `bird` (all 75 tables, one `public` schema, loaded unmodified).
2. Install NatQuery and the benchmark dependencies: `pip install -e . psycopg2-binary`.
3. Set secrets in the environment (never in files):

```bash
export GROQ_API_KEY=...            # or GROQ_API_KEYS=key1,key2 to rotate on daily limits
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=... PGDATABASE=bird
export BIRD_DATA=/path/to/MINIDEV  # folder containing mini_dev_postgresql.json
```

4. Run and score:

```bash
python benchmarks/bird/bird_bench.py run --gap 3        # about 40 min on Groq's free tier
python benchmarks/bird/bird_bench.py report
benchmarks/bird/run_official_eval.sh                    # BIRD's EX / Soft-F1 / R-VES
```

Free-tier note: Groq allows 8,000 tokens per minute and about 200,000 tokens per day per key for this model, so the runner paces calls and rotates keys. A full run needs roughly 1.1 million tokens.

## Setup used

- Model: `openai/gpt-oss-120b` on Groq, temperature 0, `reasoning_effort=low`. No fine-tuning, no examples in the prompt.
- Input: question plus BIRD's evidence hint (the leaderboard's oracle-knowledge setting).
- One benchmark role that can read all tables, so the run measures SQL quality and the cost of the security layer, not access control.
- Three variants are scored from the same LLM calls: **raw** (first LLM SQL, no security), **enforced** (that SQL through NatQuery's enforcer), **natquery** (full pipeline with up to 2 repairs).

## Data licence

BIRD data and evaluation scripts are the work of the BIRD authors (CC BY-SA 4.0). `results/` contains BIRD questions and gold SQL, so it carries the same licence. NatQuery's code is MIT.
