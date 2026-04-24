import pytest

from duckdb_sqlalchemy._validation import (
    validate_dotted_identifier,
    validate_extension_name,
    validate_identifier,
    validate_identifier_list,
)


@pytest.mark.parametrize(
    ("validator", "value", "message"),
    [
        (validate_identifier, 123, "identifier must be a string"),
        (validate_dotted_identifier, 123, "identifier must be a string"),
        (validate_extension_name, 123, "extension name must be a string"),
    ],
)
def test_validation_rejects_non_string_inputs(validator, value, message) -> None:
    with pytest.raises(ValueError, match=message):
        validator(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("validator", "value", "message"),
    [
        (validate_identifier, "bad-name", "invalid identifier: 'bad-name'"),
        (
            validate_dotted_identifier,
            "main..table",
            "invalid identifier: 'main..table'",
        ),
        (
            validate_extension_name,
            "json-httpfs",
            "invalid extension name: 'json-httpfs'",
        ),
    ],
)
def test_validation_rejects_invalid_string_formats(validator, value, message) -> None:
    with pytest.raises(ValueError, match=message):
        validator(value)


def test_validate_identifier_list_returns_tuple() -> None:
    assert validate_identifier_list(["main", "table"]) == ("main", "table")
