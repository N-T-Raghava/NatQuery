-- Authentication, implemented inside PostgreSQL (pgcrypto).
-- Failure paths RETURN nothing instead of raising, so counters and audit rows are committed.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ------------------------------------------------------------------ login
CREATE OR REPLACE FUNCTION auth.login(p_login text, p_password text, p_ip inet DEFAULT NULL, p_ua text DEFAULT NULL)
RETURNS TABLE(session_token text, account_id int, person_id int, account_type text, roles text[])
LANGUAGE plpgsql SECURITY DEFINER SET search_path = auth, public, pg_temp AS $$
DECLARE a auth.account%ROWTYPE; tok text;
BEGIN
  SELECT * INTO a FROM auth.account WHERE login_id = p_login FOR UPDATE;
  IF NOT FOUND THEN
    INSERT INTO auth.login_audit(account_id, login_id_tried, event, ip_addr, user_agent, at)
    VALUES (NULL, left(p_login, 20), 'LOGIN_FAILED', p_ip, p_ua, now());
    PERFORM pg_sleep(0.05);                       -- same cost as a wrong password
    RETURN;
  END IF;
  IF a.status <> 'ACTIVE' OR (a.locked_until IS NOT NULL AND a.locked_until > now()) THEN
    INSERT INTO auth.login_audit(account_id, login_id_tried, event, ip_addr, user_agent, at)
    VALUES (a.account_id, p_login, 'LOGIN_BLOCKED', p_ip, p_ua, now());
    RETURN;
  END IF;
  IF a.password_hash = crypt(p_password, a.password_hash) THEN
    tok := encode(gen_random_bytes(32), 'hex');
    INSERT INTO auth.session(account_id, token_hash, created_at, expires_at, ip_addr)
    VALUES (a.account_id, encode(digest(tok, 'sha256'), 'hex'), now(), now() + interval '30 minutes', p_ip);
    UPDATE auth.account SET failed_attempts = 0, locked_until = NULL, last_login_at = now() WHERE auth.account.account_id = a.account_id;
    INSERT INTO auth.login_audit(account_id, login_id_tried, event, ip_addr, user_agent, at)
    VALUES (a.account_id, p_login, 'LOGIN_OK', p_ip, p_ua, now());
    RETURN QUERY SELECT tok, a.account_id, a.person_id, a.account_type::text,
      COALESCE((SELECT array_agg(r.role_code::text ORDER BY r.priority DESC)
                FROM auth.account_role ar JOIN auth.role r USING (role_id)
                WHERE ar.account_id = a.account_id AND ar.valid_from <= current_date
                  AND (ar.valid_to IS NULL OR ar.valid_to >= current_date)), ARRAY[]::text[]);
  ELSE
    UPDATE auth.account SET failed_attempts = failed_attempts + 1,
           locked_until = CASE WHEN failed_attempts + 1 >= 5 THEN now() + interval '15 minutes' END
     WHERE auth.account.account_id = a.account_id;
    UPDATE auth.account SET failed_attempts = 0 WHERE auth.account.account_id = a.account_id AND locked_until > now();
    INSERT INTO auth.login_audit(account_id, login_id_tried, event, ip_addr, user_agent, at)
    VALUES (a.account_id, p_login, 'LOGIN_FAILED', p_ip, p_ua, now());
  END IF;
END $$;

-- ------------------------------------------------------------------ sessions
CREATE OR REPLACE FUNCTION auth.verify_session(p_token text)
RETURNS TABLE(account_id int, person_id int, account_type text, roles text[])
LANGUAGE plpgsql SECURITY DEFINER SET search_path = auth, public, pg_temp AS $$
DECLARE s auth.session%ROWTYPE;
BEGIN
  SELECT * INTO s FROM auth.session WHERE token_hash = encode(digest(p_token, 'sha256'), 'hex') AND expires_at > now() FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;
  UPDATE auth.session SET expires_at = now() + interval '30 minutes' WHERE session_id = s.session_id;   -- idle timeout slides
  RETURN QUERY
    SELECT a.account_id, a.person_id, a.account_type::text,
      COALESCE((SELECT array_agg(r.role_code::text ORDER BY r.priority DESC)
                FROM auth.account_role ar JOIN auth.role r USING (role_id)
                WHERE ar.account_id = a.account_id AND ar.valid_from <= current_date
                  AND (ar.valid_to IS NULL OR ar.valid_to >= current_date)), ARRAY[]::text[])
    FROM auth.account a WHERE a.account_id = s.account_id AND a.status = 'ACTIVE';
END $$;

CREATE OR REPLACE FUNCTION auth.logout(p_token text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = auth, public, pg_temp AS $$
DECLARE n int; aid int;
BEGIN
  DELETE FROM auth.session WHERE token_hash = encode(digest(p_token, 'sha256'), 'hex') RETURNING auth.session.account_id INTO aid;
  GET DIAGNOSTICS n = ROW_COUNT;
  IF n > 0 THEN
    INSERT INTO auth.login_audit(account_id, login_id_tried, event, at)
    SELECT aid, login_id, 'LOGOUT', now() FROM auth.account WHERE account_id = aid;
  END IF;
  RETURN n > 0;
END $$;

-- ------------------------------------------------------------------ OTP (e-mail reset / passphrase)
CREATE OR REPLACE FUNCTION auth.issue_otp(p_login text, p_purpose text) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = auth, public, pg_temp AS $$
DECLARE aid int; code text;
BEGIN
  SELECT account_id INTO aid FROM auth.account WHERE login_id = p_login AND status = 'ACTIVE';
  IF aid IS NULL THEN RETURN NULL; END IF;
  UPDATE auth.otp SET used_at = now() WHERE account_id = aid AND purpose = p_purpose AND used_at IS NULL;  -- one live code
  code := lpad((floor(random() * 1000000))::int::text, 6, '0');
  INSERT INTO auth.otp(account_id, purpose, otp_hash, expires_at)
  VALUES (aid, p_purpose, crypt(code, gen_salt('bf', 8)), now() + interval '10 minutes');
  RETURN code;                                    -- the caller e-mails it; it is never stored in clear
END $$;

CREATE OR REPLACE FUNCTION auth.verify_otp(p_login text, p_purpose text, p_code text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = auth, public, pg_temp AS $$
DECLARE o auth.otp%ROWTYPE;
BEGIN
  SELECT o2.* INTO o FROM auth.otp o2 JOIN auth.account a USING (account_id)
   WHERE a.login_id = p_login AND o2.purpose = p_purpose AND o2.used_at IS NULL AND o2.expires_at > now()
   ORDER BY o2.otp_id DESC LIMIT 1 FOR UPDATE OF o2;
  IF NOT FOUND THEN RETURN false; END IF;
  IF o.otp_hash = crypt(p_code, o.otp_hash) THEN
    UPDATE auth.otp SET used_at = now() WHERE otp_id = o.otp_id;
    RETURN true;
  END IF;
  RETURN false;
END $$;

CREATE OR REPLACE FUNCTION auth.change_password(p_login text, p_old text, p_new text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = auth, public, pg_temp AS $$
BEGIN
  IF length(p_new) < 10 OR p_new !~ '[0-9]' OR p_new !~ '[A-Za-z]' THEN RETURN false; END IF;
  UPDATE auth.account SET password_hash = crypt(p_new, gen_salt('bf', 10))
   WHERE login_id = p_login AND status = 'ACTIVE' AND password_hash = crypt(p_old, password_hash);
  IF FOUND THEN
    DELETE FROM auth.session WHERE account_id = (SELECT account_id FROM auth.account WHERE login_id = p_login);  -- sign out everywhere
    RETURN true;
  END IF;
  RETURN false;
END $$;

-- ------------------------------------------------------------------ request context (used by row-level security)
-- The application (or NatQuery) sets these per transaction:  erp.role, erp.person_id, erp.student_id,
-- erp.employee_id, erp.dept_id, erp.hall_id.  An unset context sees nothing.
CREATE OR REPLACE FUNCTION auth.ctx_role() RETURNS text LANGUAGE sql STABLE AS
$$ SELECT nullif(current_setting('erp.role', true), '') $$;
CREATE OR REPLACE FUNCTION auth.ctx_int(p_name text) RETURNS int LANGUAGE sql STABLE AS
$$ SELECT nullif(current_setting('erp.' || p_name, true), '')::int $$;

-- lock the functions down: only the application role may log people in
REVOKE ALL ON FUNCTION auth.login(text, text, inet, text), auth.verify_session(text), auth.logout(text),
  auth.issue_otp(text, text), auth.verify_otp(text, text, text), auth.change_password(text, text, text) FROM PUBLIC;
