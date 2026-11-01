# NatQuery security track (AdventureWorks)

Reproduces [`../../docs/SECURITY_REPORT.md`](../../docs/SECURITY_REPORT.md).

```bash
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=...
./setup_adventureworks.sh                  # downloads AdventureWorks, loads it, flattens it into database aw_flat
export PGDATABASE=aw_flat

python sec_bench.py sql                    # 227 hostile queries x roles through the full pipeline (no LLM, ~10 s)
python sec_bench.py auth                   # 40 auth / session / audit checks (no LLM)
python sec_bench.py raw                    # the same hostile SQL run directly on the database, read-only
export GROQ_API_KEY=...                    # never write keys into files
python sec_bench.py nl                     # compact natural-language suite, about 200K tokens
python sec_bench.py nl --full              # full suite, about 570K tokens (several free keys: GROQ_API_KEYS=k1,k2,...)
python sec_bench.py report                 # results/summary.json
```

`sql` and `auth` need no API key. The leak checker (`sec_lib.Oracle`) reads protected values straight from the tables and does not use NatQuery's policy code. `nl` reuses the pacing and key-rotation client from `../bird/bird_bench.py`.

`results/*_initial_run.*` is the first run, which found three engine bugs (see the report).

AdventureWorks is Microsoft's sample database (MIT licence); the PostgreSQL loader is Lorin Thwaits' AdventureWorks-for-Postgres.
