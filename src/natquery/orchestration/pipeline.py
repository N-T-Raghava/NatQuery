import time
from natquery.llm.client import generate_sql
from natquery.execution.engine import execute_sql
from natquery.config.settings import Settings
from natquery.observability.logger import NatQueryLogger


def run_query(user_query: str):

    conv_id = NatQueryLogger.generate_conv_id()
    db_name = Settings.get_db_config()["dbname"]

    # Log query received
    NatQueryLogger.log_event(
        level="INFO",
        event="query_received",
        db_name=db_name,
        conv_id=conv_id,
        details={"user_query": user_query},
    )

    # Generate SQL and log sql query generated
    sql = generate_sql(user_query)
    NatQueryLogger.log_event(
        level="INFO",
        event="llm_sql_generated",
        db_name=db_name,
        conv_id=conv_id,
        details={"generated_sql": sql},
    )

    # Execute SQL with timing
    start_time = time.time()

    try:
        result = execute_sql(sql)
        execution_time_ms = (time.time() - start_time) * 1000
        rows_returned = len(result)

        NatQueryLogger.log_event(
            level="INFO",
            event="db_execution_completed",
            db_name=db_name,
            conv_id=conv_id,
            details={
                "rows_returned": rows_returned,
                "execution_time_ms": execution_time_ms,
            },
        )

        # Log conversation summary
        NatQueryLogger.log_conversation(
            db_name=db_name,
            conv_id=conv_id,
            user_query=user_query,
            generated_sql=sql,
            rows_returned=rows_returned,
            execution_time_ms=execution_time_ms,
        )

        return result

    except Exception as e:

        NatQueryLogger.log_event(
            level="ERROR",
            event="db_execution_failed",
            db_name=db_name,
            conv_id=conv_id,
            details={"error": str(e)},
        )

        raise
