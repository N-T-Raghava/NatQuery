import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from natquery.llm.client import generate_sql


class TestGenerateSql:
    """Test generate_sql() function."""

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_basic(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config, temp_dir
    ):
        """Test basic SQL generation."""
        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}

        # Create schema file
        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_file = schema_dir / "schema.json"
        schema_data = {
            "tables": {
                "users": {
                    "columns": {"id": "integer", "name": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                }
            }
        }
        with open(schema_file, "w") as f:
            json.dump(schema_data, f)

        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT * FROM users;"
        mock_client.chat.completions.create.return_value = mock_response

        with patch("natquery.llm.client.Path.home", return_value=Path(temp_dir)):
            sql = generate_sql("show all users")

        assert sql == "SELECT * FROM users;"
        mock_groq_class.assert_called_once_with(api_key="test_key")
        # Verify the call was made with expected parameters
        assert mock_client.chat.completions.create.called
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs["model"] == "llama3-8b-8192"
        assert call_kwargs["temperature"] == 0.0
        assert len(call_kwargs["messages"]) == 2

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_strips_markdown(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config, temp_dir
    ):
        """Test that markdown code blocks are stripped from response."""
        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}

        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "```sql\nSELECT * FROM users;\n```"
        mock_client.chat.completions.create.return_value = mock_response

        with patch("pathlib.Path.home", return_value=Path(temp_dir)):
            sql = generate_sql("show all users")

        assert sql == "SELECT * FROM users;"

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_with_schema(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config, temp_dir
    ):
        """Test SQL generation with schema context."""
        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}

        # Create temp schema file with proper structure
        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True)
        schema_file = schema_dir / "schema.json"
        schema_data = {
            "tables": {
                "users": {
                    "columns": {"id": "integer", "name": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                },
                "posts": {
                    "columns": {"id": "integer", "title": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                },
            }
        }
        with open(schema_file, "w") as f:
            json.dump(schema_data, f)

        with patch("pathlib.Path.home", return_value=Path(temp_dir)):
            mock_client = MagicMock()
            mock_groq_class.return_value = mock_client

            mock_response = MagicMock()
            mock_response.choices = [MagicMock()]
            mock_response.choices[0].message.content = "SELECT id, name FROM users;"
            mock_client.chat.completions.create.return_value = mock_response

            generate_sql("show user names")

            # Check that schema was included in prompt
            call_args = mock_client.chat.completions.create.call_args
            system_content = call_args[1]["messages"][0]["content"]
            assert "DATABASE SCHEMA:" in system_content
            assert "users" in system_content.lower()

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_missing_schema_file(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config
    ):
        """Test SQL generation when schema file does not exist."""
        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}

        # Use a temporary directory that doesn't have schema files
        with tempfile.TemporaryDirectory() as temp_dir_path:
            with patch("pathlib.Path.home", return_value=Path(temp_dir_path)):
                mock_client = MagicMock()
                mock_groq_class.return_value = mock_client

                mock_response = MagicMock()
                mock_response.choices = [MagicMock()]
                mock_response.choices[0].message.content = "SELECT 1;"
                mock_client.chat.completions.create.return_value = mock_response

                generate_sql("test query")

                # System prompt should have DATABASE SCHEMA section
                call_args = mock_client.chat.completions.create.call_args
                system_content = call_args[1]["messages"][0]["content"]
                assert "DATABASE SCHEMA:" in system_content

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_empty_response(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config, temp_dir
    ):
        """Test handling of empty response from LLM."""
        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}

        # Create schema file
        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_file = schema_dir / "schema.json"
        with open(schema_file, "w") as f:
            json.dump({"users": ["id"]}, f)

        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "   "  # Just whitespace
        mock_client.chat.completions.create.return_value = mock_response

        with patch("natquery.llm.client.Path.home", return_value=Path(temp_dir)):
            sql = generate_sql("query")

        assert sql == ""

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_with_joins(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config, temp_dir
    ):
        """Test SQL generation for complex queries with joins."""
        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}

        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_file = schema_dir / "schema.json"
        schema_data = {
            "tables": {
                "users": {"columns": {"id": "integer"}, "primary_key": ["id"]},
                "posts": {"columns": {"id": "integer", "user_id": "integer"}},
            }
        }
        with open(schema_file, "w") as f:
            json.dump(schema_data, f)

        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = (
            "SELECT u.id, p.title FROM users u JOIN posts p ON u.id = p.user_id;"
        )
        mock_client.chat.completions.create.return_value = mock_response

        with patch("natquery.llm.client.Path.home", return_value=Path(temp_dir)):
            sql = generate_sql("get users with posts")

        assert "JOIN" in sql
        assert "users" in sql

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_uses_zero_temperature(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config, temp_dir
    ):
        """Test that generation uses temperature=0.0 for consistency."""
        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}

        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        with open(schema_dir / "schema.json", "w") as f:
            json.dump({}, f)

        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT 1;"
        mock_client.chat.completions.create.return_value = mock_response

        with patch("natquery.llm.client.Path.home", return_value=Path(temp_dir)):
            generate_sql("test")

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs["temperature"] == 0.0

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_groq_initialization(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config, temp_dir
    ):
        """Test that Groq client is properly initialized with API key."""
        api_key = "test_groq_api_key_123"
        mock_get_llm_config.return_value = {"api_key": api_key, "model": "test-model"}
        mock_get_db_config.return_value = {"dbname": "testdb"}

        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        with open(schema_dir / "schema.json", "w") as f:
            json.dump({}, f)

        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT 1;"
        mock_client.chat.completions.create.return_value = mock_response

        with patch("natquery.llm.client.Path.home", return_value=Path(temp_dir)):
            generate_sql("test")

        mock_groq_class.assert_called_once_with(api_key=api_key)

    @patch("natquery.llm.client.Settings.get_llm_config")
    @patch("natquery.llm.client.Settings.get_db_config")
    @patch("natquery.llm.client.Groq")
    def test_generate_sql_sql_cleanup(
        self, mock_groq_class, mock_get_db_config, mock_get_llm_config, temp_dir
    ):
        """Test various SQL cleanup scenarios."""
        mock_get_llm_config.return_value = {
            "api_key": "key",
            "model": "model",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}

        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        with open(schema_dir / "schema.json", "w") as f:
            json.dump({}, f)

        mock_client = MagicMock()
        mock_groq_class.return_value = mock_client

        test_cases = [
            ("```sql\nSELECT 1;\n```", "SELECT 1;"),
            ("   SELECT 1;   ", "SELECT 1;"),
            ("\n\nSELECT 1;\n\n", "SELECT 1;"),
            ("```sql\nSELECT 1;\n```   ", "SELECT 1;"),
        ]

        for response_text, expected in test_cases:
            mock_response = MagicMock()
            mock_response.choices = [MagicMock()]
            mock_response.choices[0].message.content = response_text
            mock_client.chat.completions.create.return_value = mock_response

            with patch("natquery.llm.client.Path.home", return_value=Path(temp_dir)):
                result = generate_sql("test")

            assert result == expected
