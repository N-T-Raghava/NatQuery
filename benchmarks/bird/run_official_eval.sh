#!/usr/bin/env bash
# Score predictions with BIRD's own evaluation scripts (EX, Soft-F1, R-VES).
# The scripts belong to the BIRD authors and are not redistributed here; this fetches them.
#
#   PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=... PGDATABASE=bird \
#   BIRD_DATA=/path/to/MINIDEV  ./run_official_eval.sh [predictions.json]
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PRED="$(realpath "${1:-$HERE/predictions/predict_mini_dev_PostgreSQL.json}")"
: "${BIRD_DATA:?set BIRD_DATA to the MINIDEV folder (mini_dev_postgresql.json, mini_dev_postgresql_gold.sql)}"
WORK="${WORK:-$HERE/_official}"
mkdir -p "$WORK/out"

[ -d "$WORK/mini_dev" ] || git clone --depth 1 https://github.com/bird-bench/mini_dev.git "$WORK/mini_dev"
EVAL="$WORK/mini_dev/evaluation"

# BIRD's script hard-codes its authors' Postgres login. Point it at ours through the standard PG* variables.
python3 - "$EVAL/evaluation_utils.py" <<'EOF'
import re, sys
p = sys.argv[1]
s = open(p).read()
if "PGDATABASE" not in s:
    s = re.sub(r"(def connect_postgresql\(\):.*?db = psycopg2\.connect\().*?(\)\n\s+return db)",
               r'\1dbname=os.environ.get("PGDATABASE", "bird"), user=os.environ.get("PGUSER", "postgres"), '
               r'host=os.environ.get("PGHOST", "127.0.0.1"), port=os.environ.get("PGPORT", "5432"), '
               r'password=os.environ.get("PGPASSWORD", "")\2', s, flags=re.S)
    s = "import os\n" + s
    open(p, "w").write(s)
EOF

# difficulty file the scripts expect: one JSON object per line
python3 - "$BIRD_DATA/mini_dev_postgresql.json" "$WORK/out/mini_dev_postgresql.jsonl" <<'EOF'
import json, sys
qs = json.load(open(sys.argv[1]))
open(sys.argv[2], "w").write("".join(json.dumps(q) + "\n" for q in qs))
EOF

pip install -q psycopg2-binary func_timeout numpy 2>/dev/null || true   # func_timeout may need a local copy on some systems
cd "$EVAL"
COMMON=(--sql_dialect PostgreSQL --db_root_path unused/ --num_cpus 2 --meta_time_out 30
        --predicted_sql_path "$PRED" --ground_truth_path "$BIRD_DATA/mini_dev_postgresql_gold.sql"
        --diff_json_path "$WORK/out/mini_dev_postgresql.jsonl")
python3 evaluation_ex.py  "${COMMON[@]}" --output_log_path "$WORK/out/ex.txt"
python3 evaluation_f1.py  "${COMMON[@]}" --output_log_path "$WORK/out/f1.txt"
python3 evaluation_ves.py "${COMMON[@]}" --output_log_path "$WORK/out/ves.txt"   # slow: 100 timing runs per correct answer
echo "logs in $WORK/out"
