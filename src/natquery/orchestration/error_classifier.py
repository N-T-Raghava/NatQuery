def classify_error(error_message: str) -> str:
    """Bucket a DB / validation error so the repair prompt can give a targeted hint."""
    msg = (error_message or "").lower()

    if "syntax error" in msg or "parse error" in msg:
        return "SYNTAX_ERROR"
    if (
        "unresolved reference" in msg
        or "does not exist" in msg
        or "could not be resolved" in msg
    ):
        if "column" in msg:
            return "UNDEFINED_COLUMN"
        if "relation" in msg or "table" in msg:
            return "UNDEFINED_TABLE"
    if "ambiguous" in msg:
        return "AMBIGUOUS_COLUMN"
    if "must appear in the group by clause" in msg:
        return "GROUP_BY_ERROR"
    if "missing from-clause" in msg or "missing from clause" in msg or "join" in msg:
        return "JOIN_ERROR"
    if (
        "operator does not exist" in msg
        or "invalid input syntax for type" in msg
        or "cannot be cast" in msg
    ):
        return "TYPE_ERROR"
    if "not allowed" in msg or "display-only" in msg or "not supported" in msg:
        return "POLICY"
    return "UNKNOWN"
