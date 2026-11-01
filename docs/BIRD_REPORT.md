# NatQuery on BIRD Mini-Dev (PostgreSQL)

## 1. What NatQuery is

NatQuery turns an English question into PostgreSQL SQL for people who should not need to know SQL, and does it safely enough to put in front of real users (the target is a university ERP).

The core idea: **the LLM's SQL is never executed.** It is parsed, checked, rewritten and regenerated first. Only the regenerated SQL reaches the database.

| Capability | What it does |
|---|---|
| Authentication | Pluggable providers; built-in local accounts with hashed passwords and lockout |
| Roles | student / teacher / admin (or any you define) in a YAML policy with inheritance |
| Table and column control | Hidden tables and columns are absent from the LLM prompt and fail to resolve in the enforcer |
| Row filters | Per-user filters bound from the caller's identity, wrapped around every table reference |
| Masking and redaction | Masked columns can be shown but not filtered, sorted or joined on; pattern redaction on results |
| SQL safety | SELECT-only, allow-listed functions, join and row limits, read-only timed execution |
| Repair loop | Fixable rejections go back to the LLM once or twice with a sanitised reason |
| Audit | Hash-chained, tamper-evident log of every request |
| Schema selection | Small schemas go to the LLM whole; large ones are narrowed by table ranking plus foreign-key bridging |
| Pluggable seams | Auth, database backend, LLM, rate limiter, session store, input guard |

## 2. Benchmark setup

| | |
|---|---|
| Benchmark | BIRD Mini-Dev, PostgreSQL version: 500 questions, 11 databases, 3 difficulty levels |
| Database | All 75 BIRD tables loaded unmodified, with keys |
| Model | `openai/gpt-oss-120b` (open weights) on Groq, temperature 0, low reasoning effort, no fine-tuning, no few-shot examples |
| Input | Question plus BIRD's evidence hint (leaderboard setting "oracle knowledge") |
| Security | **On.** Every query goes through the full enforcer; one benchmark role can read all tables |
| Scoring | BIRD's own official scripts: execution accuracy (EX), Soft-F1 and R-VES |
| Pipeline | One question at a time, 1 to 3 LLM calls each (first attempt plus at most 2 repairs) |

## 3. Results

### Official BIRD scores (security layer on)

| Metric | Simple (148) | Moderate (250) | Challenging (102) | **All (500)** |
|---|---|---|---|---|
| **Execution accuracy (EX)** | 58.78 | 44.00 | 30.39 | **45.60** |
| **Soft-F1** | 61.34 | 46.15 | 28.64 | **47.08** |
| **R-VES** | 58.46 | 42.92 | 28.70 | **44.62** |

228 of 500 questions exactly right under the strictest comparison BIRD has. Scoring by value with a tiny numeric tolerance (so `0.5` equals `0.5000000000000001`) gives 50.6%; the official figure above is the one to quote.

### The security layer costs nothing

| First-attempt SQL | Count |
|---|---|
| Accepted by the enforcer | 484 of 500 (96.8%) |
| Rejected | 10 (2.0%): 6 cross joins, 4 unresolved references |
| Right in raw form but wrong in the final NatQuery answer | **0** |
| Rescued by the repair loop | 3 |

Raw LLM SQL with no security scored 50.2%, the same SQL through the enforcer 50.0%, and the full pipeline 50.6% (same lenient scorer for all three). Enforcement turned no correct answer into a wrong one, and the repair loop added net accuracy.

### Leaderboard position (PostgreSQL, EX, oracle knowledge)

| Rank | System | EX |
|---|---|---|
| 1 | schemagate + Claude Opus 5.5 | 72.6 |
| 2 | SDIP + gemini-3.8-flash | 69.0 |
| 3 | Ontology2SQL | 65.8 |
| 4 | FrontierSQL + gpt-oss-120b | 56.8 |
| 4 | SmartQuery + GPT-5.1 | 56.8 |
| 6 | schemagate + Gemini 2.5 Flash | 55.6 |
| 7 | TA + GPT-4 | 50.8 |
| **8** | **NatQuery + gpt-oss-120b, security on** | **45.6** |
| 9 | GPT-4-turbo | 36.0 |
| 10 | GPT-4 | 35.8 |
| 11 | GPT-4-32k | 35.0 |
| 12 | Llama3-70b-instruct | 29.4 |
| 13 | GPT-35-turbo | 27.4 |

Scores are taken from the leaderboard screenshots; ours is self-evaluated, not submitted. **8th of 14**, ahead of every plain-model baseline: **+9.6 over GPT-4-turbo, +9.8 over GPT-4, +16.2 over Llama3-70b, +18.2 over GPT-35-turbo.** An open 120B model behind a full security layer beats GPT-4 given the same hints.

### Efficiency

Measured one question at a time; rate-limit waits excluded.

| | |
|---|---|
| End-to-end latency | **p50 1.08 s, p95 2.19 s, p99 3.13 s** |
| Answered in under 2 s | **92.6%** (41% under 1 s, 98% under 3 s) |
| LLM time | p50 1.05 s: 97.2% of latency |
| Enforcer time | **p50 4.3 ms, p95 10.8 ms, p99 15.7 ms: 0.43% of latency** |
| Database time | p50 3.5 ms (2.4% of latency) |
| LLM calls per question | **1.03** (485 questions needed one call, 14 two, 1 three) |
| Tokens per question | **2,278** (2,092 in, 186 out) |
| Tokens per correct answer | 4,995 |
| Generation speed | about 160 output tokens/s |
| Cost per question | **$0.0004** ($0.21 for all 500) |
| Cost per correct answer | $0.0009 |
| Query speed | Median executed query takes 1.08x the gold query's database time (p95 2.0x) |

Cost uses Groq's list price of $0.15 per million input and $0.60 per million output tokens.

| By difficulty | Output tokens | Latency |
|---|---|---|
| Simple | 127 | 1.05 s |
| Moderate | 191 | 1.20 s |
| Challenging | 260 | 1.39 s |

Latency grows by a third from simple to challenging questions; the security layer is flat.

### Schema selection

The prompt contained every table the gold SQL needs for **500 of 500** questions. Even with all 75 tables loaded as one 798-column schema, retrieval found the needed tables for **470 of 500**.

## 4. Limitations

1. **Self-evaluated, not on the leaderboard.** The scores come from BIRD's official scripts run by us, not from a submission. The leaderboard may also use a newer version of the Mini-Dev files than ours, so ranks are indicative.
2. **The official EX is brittle on numbers.** It compares raw database values with no tolerance, so `0.5` and `0.5000000000000001` count as different. About 25 of our 272 misses are this: right answer, different decimal representation (our own scorer, which tolerates it, gives 50.6%). FrontierSQL's published notes mention float casts in divisions as a fix; we did not apply one.
3. **Partly tuned on the test set.** The schema-selection rule was designed after reading failures on these same 500 questions. It does not use question content, but until it is checked on unseen questions, treat the result as an upper estimate.
4. **One run.** Temperature 0 is not fully deterministic on a hosted API. Expect a point or two of difference between runs; we did not measure the spread.
5. **Weak model setting.** The model ran at low reasoning effort on a free tier to keep latency and cost down. Higher effort or a stronger model would likely score more. On the same model, FrontierSQL scores 56.8 against our 45.6, an 11-point gap; we have not analysed what they do differently beyond the float-cast point above.
6. **Evidence hint is given.** Like every entry in this table, the model is handed BIRD's hint about what each term means. Real users will not provide hints, so expect lower accuracy on a real database without a good schema description.
7. **Wrong-result answers are mostly model SQL quality.** Of the misses, nearly all ran fine but returned different rows (wrong filter literal, wrong join path, extra or missing columns). BIRD's gold answers also contain annotation errors, which we did not audit, so true accuracy could be somewhat higher.
8. **No concurrency or throughput test.** Latency is one question at a time. Behaviour under many simultaneous users is unknown and needs the API layer.
9. **Security is not tested here.** This run uses one all-access role, so it shows that the enforcer does not hurt SQL quality, not that it blocks leaks. Roles, row filters, masking and leak attempts are covered by a separate AdventureWorks track.
10. **PostgreSQL only.** The enforcer is dialect-aware, but only the PostgreSQL backend exists, and the real target ERP database is unknown.
11. **Schema retrieval measured at table level.** We checked that the needed tables were in the prompt, not that the right columns were highlighted. Column-level misses would show up as wrong SQL, not as a retrieval failure.

## 5. Files

- `benchmarks/bird/bird_bench.py`: the runner (`run` resumes, `report` rebuilds the summary)
- `benchmarks/bird/results/`: `results.jsonl` (per-question record), `results.csv`, `summary.json`
- `benchmarks/bird/predictions/predict_mini_dev_PostgreSQL.json`: answers in BIRD's submission format
- `benchmarks/bird/official_logs/`: output of BIRD's scoring scripts
- `benchmarks/bird/README.md`: how to reproduce
