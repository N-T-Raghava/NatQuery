-- Database roles, least-privilege grants and row-level security (second line of defence).
--
--   erp_owner    owns every object; used for migrations only (NOLOGIN)
--   erp_app      the ERP application: DML on business tables, EXECUTE on auth functions
--   erp_ro       what NatQuery may do: SELECT only, no secret columns, row-level security applies
--   natquery     the login NatQuery connects with; it can only SET ROLE erp_ro

-- ------------------------------------------------------------------ ownership
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT nspname FROM pg_namespace WHERE nspname IN ('auth','org','people','acad','fin','lib','svc','gh','cdc','sys') LOOP
    EXECUTE format('ALTER SCHEMA %I OWNER TO erp_owner', r.nspname);
  END LOOP;
  FOR r IN SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('auth','org','people','acad','fin','lib','svc','gh','cdc','sys') LOOP
    EXECUTE format('ALTER TABLE %I.%I OWNER TO erp_owner', r.schemaname, r.tablename);
  END LOOP;
  FOR r IN SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE c.relkind = 'S' AND n.nspname IN ('auth','org','people','acad','fin','lib','svc','gh','cdc','sys') LOOP
    EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO erp_owner', r.nspname, r.relname);
  END LOOP;
  FOR r IN SELECT p.oid::regprocedure AS sig FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'auth' LOOP
    EXECUTE format('ALTER FUNCTION %s OWNER TO erp_owner', r.sig);
  END LOOP;
END $$;

-- ------------------------------------------------------------------ application role
GRANT USAGE ON SCHEMA auth, org, people, acad, fin, lib, svc, gh, cdc, sys TO erp_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA org, people, acad, fin, lib, svc, gh, cdc, sys TO erp_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA org, people, acad, fin, lib, svc, gh, cdc, sys, auth TO erp_app;
GRANT EXECUTE ON FUNCTION auth.login(text, text, inet, text), auth.verify_session(text), auth.logout(text),
  auth.issue_otp(text, text), auth.verify_otp(text, text, text), auth.change_password(text, text, text) TO erp_app;
GRANT SELECT ON auth.role, auth.account_role TO erp_app;
GRANT SELECT, INSERT ON auth.login_audit TO erp_app;
-- erp_app never reads password hashes, tokens or OTP hashes directly: only through the functions above.

-- ------------------------------------------------------------------ read-only role for NatQuery
GRANT USAGE ON SCHEMA org, people, acad, fin, lib, svc, gh, cdc, sys, auth TO erp_ro;
GRANT EXECUTE ON FUNCTION auth.ctx_role(), auth.ctx_int(text) TO erp_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA org, people, acad, fin, lib, svc, gh, cdc, sys TO erp_ro;
-- auth: only the non-secret tables, and only the non-secret columns of account
GRANT SELECT ON auth.role, auth.account_role, auth.login_audit TO erp_ro;
GRANT SELECT (account_id, login_id, person_id, account_type, status, failed_attempts, locked_until, last_login_at, created_at)
  ON auth.account TO erp_ro;
-- (no grant at all on auth.session and auth.otp)

-- ------------------------------------------------------------------ row-level security helpers
-- SECURITY DEFINER so they can read people.student even though row-level security applies to erp_ro.
CREATE OR REPLACE FUNCTION auth.student_in_scope(p_student_id int) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = people, acad, cdc, auth, pg_temp AS $$
  SELECT CASE auth.ctx_role()
    WHEN 'admin' THEN true
    WHEN 'acad_section' THEN true
    WHEN 'student' THEN p_student_id = auth.ctx_int('student_id')
    WHEN 'faculty' THEN
         EXISTS (SELECT 1 FROM people.faculty_advisor fa WHERE fa.student_id = p_student_id
                  AND fa.employee_id = auth.ctx_int('employee_id') AND fa.to_session_id IS NULL)
      OR EXISTS (SELECT 1 FROM acad.registration r JOIN acad.offering_instructor oi USING (offering_id)
                  WHERE r.student_id = p_student_id AND oi.employee_id = auth.ctx_int('employee_id'))
    WHEN 'hod' THEN EXISTS (SELECT 1 FROM people.student s WHERE s.student_id = p_student_id AND s.department_id = auth.ctx_int('dept_id'))
    WHEN 'dept_office' THEN EXISTS (SELECT 1 FROM people.student s WHERE s.student_id = p_student_id AND s.department_id = auth.ctx_int('dept_id'))
    WHEN 'hall_office' THEN EXISTS (SELECT 1 FROM people.student s WHERE s.student_id = p_student_id AND s.hall_id = auth.ctx_int('hall_id'))
    WHEN 'cdc' THEN EXISTS (SELECT 1 FROM cdc.job_application ja WHERE ja.student_id = p_student_id)
    ELSE false END
$$;

CREATE OR REPLACE FUNCTION auth.registration_in_scope(p_registration_id int) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = acad, auth, pg_temp AS $$
  SELECT EXISTS (SELECT 1 FROM acad.registration r WHERE r.registration_id = p_registration_id AND auth.student_in_scope(r.student_id))
$$;

CREATE OR REPLACE FUNCTION auth.person_in_scope(p_person_id int) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = people, auth, pg_temp AS $$
  SELECT p_person_id = auth.ctx_int('person_id')
      OR auth.ctx_role() IN ('admin', 'accounts', 'library')
      OR EXISTS (SELECT 1 FROM people.student s WHERE s.person_id = p_person_id AND auth.student_in_scope(s.student_id))
      OR (auth.ctx_role() IN ('hod', 'dept_office')
          AND EXISTS (SELECT 1 FROM people.employee e WHERE e.person_id = p_person_id AND e.department_id = auth.ctx_int('dept_id')))
$$;
GRANT EXECUTE ON FUNCTION auth.student_in_scope(int), auth.registration_in_scope(int), auth.person_in_scope(int) TO erp_ro;

-- ------------------------------------------------------------------ policies
DO $$
DECLARE r record;
BEGIN
  -- tables keyed by student_id
  FOR r IN SELECT * FROM (VALUES
      ('people','student','student_id'), ('people','guardian','student_id'), ('acad','registration','student_id'),
      ('acad','semester_performance','student_id'), ('acad','medical_leave','student_id'),
      ('svc','counselling_session','student_id'), ('fin','fee_demand','student_id'), ('fin','payment','student_id'),
      ('fin','family_income_declaration','student_id'), ('fin','mediclaim_claim','student_id'),
      ('fin','scholarship_award','student_id')) AS t(s, tbl, col) LOOP
    EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', r.s, r.tbl);
    EXECUTE format('CREATE POLICY erp_ro_scope ON %I.%I FOR SELECT TO erp_ro USING (%s)', r.s, r.tbl,
      CASE WHEN r.s = 'fin' THEN   -- money: the student themself, the accounts section, and admin (faculty, HoD, hall, CDC see none)
             format('auth.ctx_role() = ''accounts'' OR (auth.ctx_role() IN (''student'',''admin'') AND auth.student_in_scope(%I))', r.col)
           WHEN r.tbl = 'student' THEN   -- accounts and library need to identify students (NatQuery limits them to a few columns)
             format('auth.student_in_scope(%I) OR auth.ctx_role() IN (''accounts'',''library'')', r.col)
           WHEN r.tbl = 'counselling_session' THEN format('auth.ctx_role() IN (''student'',''admin'') AND auth.student_in_scope(%I)', r.col)
           ELSE format('auth.student_in_scope(%I)', r.col) END);
  END LOOP;
  -- result is keyed by registration
  ALTER TABLE acad.result ENABLE ROW LEVEL SECURITY;
  CREATE POLICY erp_ro_scope ON acad.result FOR SELECT TO erp_ro USING (auth.registration_in_scope(registration_id));
  -- people.person, identity, bank, address, emergency contact: keyed by person
  FOR r IN SELECT * FROM (VALUES ('people','person','person_id'), ('people','person_identity','person_id'),
      ('people','bank_account','person_id'), ('people','address','person_id'), ('people','emergency_contact','person_id')) AS t(s, tbl, col) LOOP
    EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', r.s, r.tbl);
    EXECUTE format('CREATE POLICY erp_ro_scope ON %I.%I FOR SELECT TO erp_ro USING (%s)', r.s, r.tbl,
      CASE WHEN r.tbl IN ('person_identity','bank_account')
           THEN format('%I = auth.ctx_int(''person_id'') OR auth.ctx_role() IN (''admin'',''accounts'')', r.col)
           ELSE format('auth.person_in_scope(%I)', r.col) END);
  END LOOP;
  -- salary: own slips, or accounts/admin
  ALTER TABLE fin.salary_slip ENABLE ROW LEVEL SECURITY;
  CREATE POLICY erp_ro_scope ON fin.salary_slip FOR SELECT TO erp_ro
    USING (employee_id = auth.ctx_int('employee_id') OR auth.ctx_role() IN ('accounts','admin'));
  -- library loans: own, or library staff / admin
  ALTER TABLE lib.loan ENABLE ROW LEVEL SECURITY;
  CREATE POLICY erp_ro_scope ON lib.loan FOR SELECT TO erp_ro
    USING (person_id = auth.ctx_int('person_id') OR auth.ctx_role() IN ('library','admin'));
  ALTER TABLE lib.library_due ENABLE ROW LEVEL SECURITY;
  CREATE POLICY erp_ro_scope ON lib.library_due FOR SELECT TO erp_ro
    USING (person_id = auth.ctx_int('person_id') OR auth.ctx_role() IN ('library','admin'));
END $$;
-- The owner and superuser bypass these policies (migrations, seeding). The ERP application role is given an
-- explicit allow-all policy on every protected table: it enforces access in application code, as the real ERP does.
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT schemaname, tablename FROM pg_policies WHERE policyname = 'erp_ro_scope' LOOP
    EXECUTE format('CREATE POLICY erp_app_all ON %I.%I FOR ALL TO erp_app USING (true) WITH CHECK (true)', r.schemaname, r.tablename);
  END LOOP;
END $$;

-- natquery logs in, then drops to erp_ro per query (SET LOCAL ROLE)
GRANT erp_ro TO natquery;
