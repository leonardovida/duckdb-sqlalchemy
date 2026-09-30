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
    "rate limit",
)
# Transient HTTP statuses, as "HTTP Error: 503 ..." or as httpfs reports them:
# "HTTP Error: HTTP GET error on '<url>' (HTTP 503)".
HTTP_TRANSIENT_STATUS_PATTERN = re.compile(
    r"(?:\(http |http error: )(?:429|502|503|504)\b"
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
# Functions with side effects that a read-shaped statement can still call:
# sequence advancement and MotherDuck md_* functions such as md_run_job().
SIDE_EFFECT_FUNCTION_PATTERN = re.compile(r"\b(?:nextval|setval|md_\w*)\"?\s*\(")


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
    # Fail closed for lexical forms this lightweight scanner does not parse.
    # Dollar strings, escape strings, and nested comments can conceal a
    # statement separator from the ordinary quote/comment handling below.
    if (
        re.search(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", statement)
        or "\\" in statement
        or statement.count("/*") > 1
    ):
        return False
    normalized = _strip_leading_sql_comments(statement)
    if not normalized or _has_additional_sql_statement(normalized):
        return False
    normalized = normalized.lower()
    # ANALYZE executes its argument, including INSERT/UPDATE/DELETE.
    if normalized.startswith("explain") and re.search(r"\banalyze\b", normalized):
        return False
    if SIDE_EFFECT_FUNCTION_PATTERN.search(normalized):
        return False
    if normalized.startswith(IDEMPOTENT_STATEMENT_PREFIXES):
        return True
    if not normalized.startswith("with"):
        return False
    return MUTATING_STATEMENT_PATTERN.search(normalized) is None


def _is_transient_error(error: BaseException) -> bool:
    message = str(error).lower()
    # A "504 Gateway Timeout" is a response from the server, not a lost
    # connection, so it does not match the "timeout" disconnect pattern.
    connection_message = message.replace("gateway timeout", "")
    if any(pattern in connection_message for pattern in DISCONNECT_ERROR_PATTERNS):
        return False
    return (
        any(pattern in message for pattern in TRANSIENT_ERROR_PATTERNS)
        or HTTP_TRANSIENT_STATUS_PATTERN.search(message) is not None
    )


def _is_aborted_transaction_error(error: BaseException) -> bool:
    return "current transaction is aborted" in str(error).lower()


def _top_level_sql_words(statement: str) -> list[str]:
    """Read operation keywords without entering literals, comments or CTE bodies."""
    words: list[str] = []
    depth = 0
    index = 0
    while index < len(statement):
        if statement.startswith("--", index):
            end = statement.find("\n", index + 2)
            if end < 0:
                break
            index = end + 1
        elif statement.startswith("/*", index):
            nesting = 1
            index += 2
            while index < len(statement) and nesting:
                if statement.startswith("/*", index):
                    nesting += 1
                    index += 2
                elif statement.startswith("*/", index):
                    nesting -= 1
                    index += 2
                else:
                    index += 1
            if nesting:
                return []
        elif statement[index] in "'\"":
            quote = statement[index]
            index += 1
            while index < len(statement):
                if statement[index] == quote:
                    if statement[index : index + 2] == quote * 2:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
        elif statement[index] == "$":
            match = re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", statement[index:])
            if match:
                delimiter = match.group()
                end = statement.find(delimiter, index + len(delimiter))
                if end < 0:
                    return []
                index = end + len(delimiter)
            else:
                index += 1
        elif statement[index] == "(":
            depth += 1
            index += 1
        elif statement[index] == ")":
            depth -= 1
            index += 1
        elif statement[index] == ";" and depth == 0:
            if _strip_leading_sql_comments(statement[index + 1 :]).strip("; \t\r\n"):
                return []
            break
        elif statement[index].isalpha() or statement[index] == "_":
            end = index + 1
            while end < len(statement) and (
                statement[end].isalnum() or statement[end] in "_$"
            ):
                end += 1
            if depth == 0:
                words.append(statement[index:end].lower())
            index = end
        else:
            index += 1
    return words if depth == 0 else []
