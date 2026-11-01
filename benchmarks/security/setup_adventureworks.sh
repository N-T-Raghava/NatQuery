#!/usr/bin/env bash
# Build the AdventureWorks 2014 OLTP database used by the security track, flattened into one `public` schema
# (NatQuery's PostgreSQL backend reads one schema). Needs: psql, ruby, curl, unzip, and a Postgres you can create databases in.
#
#   PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=... ./setup_adventureworks.sh
#
# Data: Microsoft AdventureWorks (MIT licence). Loader: Lorin Thwaits' AdventureWorks-for-Postgres.
set -euo pipefail
W="${WORK:-$(pwd)/_aw}"; mkdir -p "$W"; cd "$W"
curl -sL -o oltp.zip https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks-oltp-install-script.zip
unzip -qo oltp.zip -d oltp && cp oltp/*.csv .
for f in install.sql update_csvs.rb; do
  curl -s -o "$f" "https://raw.githubusercontent.com/lorint/AdventureWorks-for-Postgres/master/$f"
done
ruby update_csvs.rb >/dev/null
psql -qc 'DROP DATABASE IF EXISTS "Adventureworks"' -c 'CREATE DATABASE "Adventureworks"'
psql -q -d Adventureworks < install.sql >load.log 2>&1
psql -qc 'DROP DATABASE IF EXISTS aw_flat' -c 'CREATE DATABASE aw_flat TEMPLATE "Adventureworks"'
psql -q -d aw_flat <<'SQL'
DROP SCHEMA hr, pe, pr, pu, sa CASCADE;
DO $$ DECLARE r record; BEGIN
 FOR r IN SELECT table_schema s, table_name t FROM information_schema.views
          WHERE table_schema IN ('humanresources','person','production','purchasing','sales') LOOP
   EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', r.s, r.t); END LOOP;
 FOR r IN SELECT schemaname s, tablename t FROM pg_tables
          WHERE schemaname IN ('humanresources','person','production','purchasing','sales') LOOP
   EXECUTE format('ALTER TABLE %I.%I SET SCHEMA public', r.s, r.t); END LOOP;
END $$;
SQL
echo "aw_flat ready: $(psql -d aw_flat -Atc "select count(*) from information_schema.tables where table_schema='public'") tables"
