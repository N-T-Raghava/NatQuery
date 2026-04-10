"""Unit tests for prompt/builder.py"""

from natquery.prompt.builder import build_prompt


class TestBuildPrompt:
    """Test build_prompt() function."""

    def test_build_prompt_basic(self):
        """Test basic prompt building."""
        user_query = "show all users"
        schema_text = "TABLE users (id INTEGER, name VARCHAR)"

        result = build_prompt(user_query, schema_text)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["role"] == "system"
        assert result[1]["role"] == "user"

    def test_build_prompt_system_message(self):
        """Test system message contains correct content."""
        user_query = "count posts"
        schema_text = "TABLE posts (id INTEGER, title VARCHAR)"

        result = build_prompt(user_query, schema_text)

        system_msg = result[0]["content"]
        assert "PostgreSQL" in system_msg
        assert "STRICT RULES" in system_msg
        assert schema_text in system_msg

    def test_build_prompt_user_message(self):
        """Test user message contains query."""
        user_query = "get all active users"
        schema_text = "TABLE users (id INTEGER)"

        result = build_prompt(user_query, schema_text)

        user_msg = result[1]["content"]
        assert user_query in user_msg

    def test_build_prompt_forbidden_keywords_in_system_prompt(self):
        """Test that system prompt prohibits forbidden keywords."""
        schema = "TABLE products"

        result = build_prompt("query", schema)

        system_content = result[0]["content"]
        # Check for forbidden keyword warnings
        assert any(
            keyword in system_content.lower()
            for keyword in ["insert", "update", "delete", "drop", "alter", "truncate"]
        )

    def test_build_prompt_with_empty_schema(self):
        """Test prompt building with empty schema."""
        user_query = "select something"
        schema_text = ""

        result = build_prompt(user_query, schema_text)

        assert len(result) == 2
        assert result[1]["content"] == user_query.strip()

    def test_build_prompt_with_complex_schema(self):
        """Test prompt building with complex multi-table schema."""
        user_query = "join users and posts"
        schema_text = """TABLE users (
  id INTEGER PRIMARY KEY,
  name VARCHAR
)

TABLE posts (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  title VARCHAR
)

RELATIONSHIPS:
posts.user_id → users.id"""

        result = build_prompt(user_query, schema_text)

        system_msg = result[0]["content"]
        assert "users" in system_msg
        assert "posts" in system_msg
        assert schema_text in system_msg

    def test_build_prompt_user_query_whitespace_stripped(self):
        """Test that user query whitespace is stripped."""
        user_query = "   select * from users   "
        schema_text = "TABLE users"

        result = build_prompt(user_query, schema_text)

        assert result[1]["content"] == "select * from users"

    def test_build_prompt_system_message_whitespace_stripped(self):
        """Test that system message is stripped."""
        schema = "TABLE test"
        result = build_prompt("query", schema)

        system_content = result[0]["content"]
        assert system_content == system_content.strip()

    def test_build_prompt_message_roles_correct(self):
        """Test that message roles are correctly set."""
        result = build_prompt("test query", "test schema")

        assert result[0]["role"] == "system"
        assert result[1]["role"] == "user"
        assert all("role" in msg for msg in result)

    def test_build_prompt_message_content_exists(self):
        """Test that all messages have content."""
        result = build_prompt("query", "schema")

        assert all("content" in msg for msg in result)
        assert all(len(msg["content"]) > 0 for msg in result)

    def test_build_prompt_multiline_query(self):
        """Test prompt with multiline user query."""
        user_query = """
        SELECT id, name
        FROM users
        WHERE status = 'active'
        """
        schema_text = "TABLE users"

        result = build_prompt(user_query, schema_text)

        # Query should be preserved but whitespace trimmed
        assert result[1]["content"].startswith("SELECT")

    def test_build_prompt_schema_escaping(self):
        """Test that schema with special characters is handled."""
        schema_text = 'TABLE "users" ("user_id" INTEGER, "user-name" VARCHAR)'
        user_query = "select from users"

        result = build_prompt(user_query, schema_text)

        system_msg = result[0]["content"]
        assert schema_text in system_msg

    def test_build_prompt_special_characters_in_query(self):
        """Test query with special characters."""
        user_query = "get users where email like '%@example.com'"
        schema_text = "TABLE users"

        result = build_prompt(user_query, schema_text)

        assert user_query in result[1]["content"]

    def test_build_prompt_output_format(self):
        """Test output format is list of dicts."""
        result = build_prompt("query", "schema")

        assert isinstance(result, list)
        assert all(isinstance(msg, dict) for msg in result)
        assert all(set(msg.keys()) == {"role", "content"} for msg in result)

    def test_build_prompt_only_select_emphasis(self):
        """Test that prompt emphasizes SELECT-only focus."""
        schema = "TABLE users"
        result = build_prompt("test", schema)

        system_content = result[0]["content"]
        # Should mention INSERT, UPDATE, DELETE restrictions
        assert "INSERT" in system_content or "insert" in system_content.lower()
        assert "UPDATE" in system_content or "update" in system_content.lower()
        assert "DELETE" in system_content or "delete" in system_content.lower()
