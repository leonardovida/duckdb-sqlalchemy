import re

DISCONNECT_ERROR_PATTERNS = (
    "connection closed",
    "connection reset",
    "connection refused",
    "broken pipe",
    "socket",
    "network is unreachable",
    "timed out",
    "timeout",
    "could not connect",
    "failed to connect",
)

TRANSIENT_ERROR_PATTERNS = (
    "temporarily unavailable",
    "service unavailable",
    "http error: 429",
    "http error: 503",
    "http error: 504",
    "rate limit",
)

IDEMPOTENT_STATEMENT_PREFIXES = (
    "select",
    "show",
    "describe",
    "pragma",
    "explain",
    "values",
)
MUTATING_STATEMENT_PATTERN = re.compile(
    r"\b("
    r"insert|update|delete|merge|copy|create|alter|drop|grant|revoke|truncate|"
    r"call|attach|detach"
    r")\b"
)


def _strip_leading_sql_comments(statement: str) -> str:
    sql = statement.lstrip()
    while sql:
        if sql.startswith("--"):
            newline_index = sql.find("\n")
            if newline_index == -1:
                return ""
            sql = sql[newline_index + 1 :].lstrip()
            continue
        if sql.startswith("/*"):
            comment_end = sql.find("*/", 2)
            if comment_end == -1:
                return ""
            sql = sql[comment_end + 2 :].lstrip()
            continue
        break
    return sql


def _is_idempotent_statement(statement: str) -> bool:
    normalized = _strip_leading_sql_comments(statement).lower()
    if not normalized:
        return False
    if normalized.startswith(IDEMPOTENT_STATEMENT_PREFIXES):
        return True
    if not normalized.startswith("with"):
        return False
    return MUTATING_STATEMENT_PATTERN.search(normalized) is None


def _is_transient_error(error: BaseException) -> bool:
    message = str(error).lower()
    if any(pattern in message for pattern in DISCONNECT_ERROR_PATTERNS):
        return False
    return any(pattern in message for pattern in TRANSIENT_ERROR_PATTERNS)
