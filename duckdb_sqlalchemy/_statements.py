import re

DISCONNECT_ERROR_PATTERNS = (
    "connection already closed",
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


def _has_additional_sql_statement(statement: str) -> bool:
    index = 0
    while index < len(statement):
        char = statement[index]
        if char in {"'", '"'}:
            quote = char
            index += 1
            while index < len(statement):
                if statement[index] != quote:
                    index += 1
                    continue
                if index + 1 < len(statement) and statement[index + 1] == quote:
                    index += 2
                    continue
                index += 1
                break
            continue
        if statement.startswith("--", index):
            newline_index = statement.find("\n", index + 2)
            if newline_index == -1:
                return False
            index = newline_index + 1
            continue
        if statement.startswith("/*", index):
            comment_end = statement.find("*/", index + 2)
            if comment_end == -1:
                return False
            index = comment_end + 2
            continue
        if char == ";":
            remainder = _strip_leading_sql_comments(statement[index + 1 :])
            while remainder.startswith(";"):
                remainder = _strip_leading_sql_comments(remainder[1:])
            return bool(remainder)
        index += 1
    return False


def _is_idempotent_statement(statement: str) -> bool:
    normalized = _strip_leading_sql_comments(statement)
    if not normalized or _has_additional_sql_statement(normalized):
        return False
    normalized = normalized.lower()
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
