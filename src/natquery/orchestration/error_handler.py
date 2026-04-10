from natquery.llm.self_corrector import correct_sql
from natquery.security.validator import validate_sql
from natquery.observability.logger import NatQueryLogger
from natquery.orchestration.error_classifier import classify_error


def handle_query_error(
    user_query: str,
    sql: str,
    error: Exception,
    conv_id: str,
    db_name: str,
    max_retries: int = 3,
):
    """
    Retry mechanism with intelligent self-correction.
    """

    current_sql = sql
    last_error = error
    corrected_sql = None

    for attempt in range(1, max_retries + 1):

        error_type = classify_error(str(last_error))

        try:
            corrected_sql = correct_sql(
                user_query,
                current_sql,
                str(last_error),
            )

            if not corrected_sql or len(corrected_sql.strip()) == 0:
                raise ValueError("Empty SQL from self-corrector")

            NatQueryLogger.log_event(
                level="INFO",
                event="sql_correction_attempt",
                db_name=db_name,
                conv_id=conv_id,
                details={
                    "attempt": attempt,
                    "error_type": error_type,
                    "previous_sql": current_sql,
                    "corrected_sql": corrected_sql,
                },
            )

            # Validate corrected SQL
            validate_sql(corrected_sql)

            return corrected_sql  # success

        except Exception as e:

            NatQueryLogger.log_event(
                level="WARNING",
                event="sql_correction_failed",
                db_name=db_name,
                conv_id=conv_id,
                details={
                    "attempt": attempt,
                    "error_type": error_type,
                    "error": str(e),
                    "previous_sql": current_sql,
                },
            )

            current_sql = corrected_sql if corrected_sql else current_sql
            last_error = e

    NatQueryLogger.log_event(
        level="ERROR",
        event="self_correction_exhausted",
        db_name=db_name,
        conv_id=conv_id,
        details={"final_error": str(last_error)},
    )

    raise RuntimeError("Max retries reached. Query failed after corrections.")
