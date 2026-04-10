"""Unit tests for llm/self_corrector.py"""

from unittest.mock import patch, MagicMock
from natquery.llm.self_corrector import correct_sql


class TestCorrectSql:
    """Test correct_sql() function."""

    @patch("natquery.llm.self_corrector.Groq")
    @patch("natquery.llm.self_corrector.Settings.get_llm_config")
    @patch("natquery.llm.self_corrector.Settings.get_db_config")
    @patch("natquery.llm.self_corrector.classify_error")
    def test_correct_sql_syntax_error(
        self,
        mock_classify,
        mock_get_db_config,
        mock_get_llm_config,
        mock_groq_class,
        temp_dir,
    ):
        """Test SQL correction for syntax errors."""
        from pathlib import Path

        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_classify.return_value = "SYNTAX_ERROR"

        # Create mock schema
        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_file = schema_dir / "schema.json"
        import json

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

        with patch(
            "natquery.llm.self_corrector.Path.home", return_value=Path(temp_dir)
        ):
            corrected = correct_sql(
                "show users",
                "SELEC * FROM users",
                "syntax error at or near 'SELEC'",
            )

        assert corrected == "SELECT * FROM users;"
        assert (
            "SYNTAX_ERROR"
            in mock_client.chat.completions.create.call_args[1]["messages"][0][
                "content"
            ]
        )

    @patch("natquery.llm.self_corrector.Groq")
    @patch("natquery.llm.self_corrector.Settings.get_llm_config")
    @patch("natquery.llm.self_corrector.Settings.get_db_config")
    @patch("natquery.llm.self_corrector.classify_error")
    def test_correct_sql_undefined_column(
        self,
        mock_classify,
        mock_get_db_config,
        mock_get_llm_config,
        mock_groq_class,
        temp_dir,
    ):
        """Test SQL correction for undefined column errors."""
        from pathlib import Path

        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_classify.return_value = "UNDEFINED_COLUMN"

        # Create mock schema
        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_file = schema_dir / "schema.json"
        import json

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
        mock_response.choices[0].message.content = "SELECT id, name FROM users;"
        mock_client.chat.completions.create.return_value = mock_response

        with patch(
            "natquery.llm.self_corrector.Path.home", return_value=Path(temp_dir)
        ):
            corrected = correct_sql(
                "get users",
                "SELECT user_id FROM users;",
                'column "user_id" does not exist',
            )

        assert corrected == "SELECT id, name FROM users;"
        assert (
            "UNDEFINED_COLUMN"
            in mock_client.chat.completions.create.call_args[1]["messages"][0][
                "content"
            ]
        )

    @patch("natquery.llm.self_corrector.Groq")
    @patch("natquery.llm.self_corrector.Settings.get_llm_config")
    @patch("natquery.llm.self_corrector.Settings.get_db_config")
    @patch("natquery.llm.self_corrector.classify_error")
    def test_correct_sql_strips_markdown(
        self,
        mock_classify,
        mock_get_db_config,
        mock_get_llm_config,
        mock_groq_class,
        temp_dir,
    ):
        """Test that markdown blocks are stripped from corrected SQL."""
        from pathlib import Path

        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_classify.return_value = "SYNTAX_ERROR"

        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_file = schema_dir / "schema.json"
        import json

        schema_data = {
            "tables": {
                "users": {
                    "columns": {"id": "integer"},
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
        mock_response.choices[0].message.content = "```sql\nSELECT * FROM users;\n```"
        mock_client.chat.completions.create.return_value = mock_response

        with patch(
            "natquery.llm.self_corrector.Path.home", return_value=Path(temp_dir)
        ):
            corrected = correct_sql(
                "get all users",
                "SELEC * FROM users",
                "syntax error",
            )

        assert corrected == "SELECT * FROM users;"

    @patch("natquery.llm.self_corrector.Groq")
    @patch("natquery.llm.self_corrector.Settings.get_llm_config")
    @patch("natquery.llm.self_corrector.Settings.get_db_config")
    @patch("natquery.llm.self_corrector.classify_error")
    def test_correct_sql_with_join_error(
        self,
        mock_classify,
        mock_get_db_config,
        mock_get_llm_config,
        mock_groq_class,
        temp_dir,
    ):
        """Test SQL correction for join errors."""
        from pathlib import Path

        mock_get_llm_config.return_value = {
            "api_key": "test_key",
            "model": "llama3-8b-8192",
        }
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_classify.return_value = "JOIN_ERROR"

        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_file = schema_dir / "schema.json"
        import json

        schema_data = {
            "tables": {
                "users": {
                    "columns": {"id": "integer"},
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
        mock_response.choices[0].message.content = "SELECT u.id FROM users u;"
        mock_client.chat.completions.create.return_value = mock_response

        with patch(
            "natquery.llm.self_corrector.Path.home", return_value=Path(temp_dir)
        ):
            corrected = correct_sql(
                "get data",
                "SELECT * FROM WHERE id = 1;",
                "missing FROM clause",
            )

        assert corrected == "SELECT u.id FROM users u;"
        assert (
            "JOIN_ERROR"
            in mock_client.chat.completions.create.call_args[1]["messages"][0][
                "content"
            ]
        )
