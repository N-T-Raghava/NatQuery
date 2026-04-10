def classify_error(error_message: str) -> str:
    """
    Classify PostgreSQL errors into categories.
    """

    msg = error_message.lower()

    if "syntax error" in msg:
        return "SYNTAX_ERROR"

    if "does not exist" in msg:
        if "column" in msg:
            return "UNDEFINED_COLUMN"
        if "relation" in msg or "table" in msg:
            return "UNDEFINED_TABLE"

    if "ambiguous" in msg:
        return "AMBIGUOUS_COLUMN"

    if "operator does not exist" in msg or "type" in msg:
        return "TYPE_ERROR"

    if "must appear in the group by clause" in msg:
        return "GROUP_BY_ERROR"

    if "missing from clause" in msg:
        return "JOIN_ERROR"

    return "UNKNOWN"
