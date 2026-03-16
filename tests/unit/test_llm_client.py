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
        schema_data = {"users": ["id", "name"]}
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
        mock_client.chat.completions.create.assert_called_once_with(
            model="llama3-8b-8192",
            messages=[
                {
                    "role": "system",
                    "content": "\n    You are a PostgreSQL expert.\n\n    Database Schema:\n    \nTable: users\nColumns: id, name\n\n\n    Convert user request into valid PostgreSQL SQL.\n    Only return SQL.\n    ",
                },
                {"role": "user", "content": "show all users"},
            ],
            temperature=0.0,
        )

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

        # Create temp schema file
        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True)
        schema_file = schema_dir / "schema.json"
        schema_data = {"users": ["id", "name"], "posts": ["id", "title"]}
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
            assert "Table: users" in system_content
            assert "Columns: id, name" in system_content

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

                # System prompt should have empty schema
                call_args = mock_client.chat.completions.create.call_args
                system_content = call_args[1]["messages"][0]["content"]
                assert "Database Schema:\n    \n" in system_content
