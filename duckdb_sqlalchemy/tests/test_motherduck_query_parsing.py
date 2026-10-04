from typing import Any

import pytest

from duckdb_sqlalchemy import Dialect, MotherDuckURL
from duckdb_sqlalchemy.motherduck import append_query_to_database


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        ("", {}),
        ("bare&empty=", {"bare": "", "empty": ""}),
        ("label=&label=two&label=", {"label": ("", "two", "")}),
        ("label=a%2Bb&label=hello+world", {"label": ("a+b", "hello world")}),
        ("%E9%9B%AA=%E2%98%83", {"雪": "☃"}),
        ("label=%FF&label=%25", {"label": ("\ufffd", "%")}),
        ("label=one;other=two", {"label": "one;other=two"}),
        ("&&=unnamed&bare&&", {"": "unnamed", "bare": ""}),
    ],
)
def test_embedded_query_retains_decoded_values(
    query_string: str, expected: dict[str, Any]
) -> None:
    url = MotherDuckURL(database=f"md:analytics?{query_string}")

    assert url.database == "md:analytics"
    assert dict(url.query) == expected
    assert list(url.query) == list(expected)


def test_embedded_query_precedence_and_connect_routing() -> None:
    query = {"label": ("new", ""), "threads": None, "session_name": "query"}
    path_query = {"session_name": ("path", "")}
    url = MotherDuckURL(
        database="md:analytics?session_name=first&threads=2&label=old&label=older&bare",
        query=query,
        path_query=path_query,
        session_name=("final", ""),
    )

    # None does not erase an embedded setting. Explicit values replace the
    # complete repeated-value group; routing leaves config key order intact.
    assert list(url.query.items()) == [
        ("threads", "2"),
        ("label", ("new", "")),
        ("bare", ""),
        ("session_name", ("final", "")),
    ]
    assert Dialect().create_connect_args(url) == (
        (),
        {
            "database": "md:analytics?session_name=final&session_name=",
            "url_config": {"threads": "2", "label": ("new", ""), "bare": ""},
        },
    )
    assert query == {"label": ("new", ""), "threads": None, "session_name": "query"}
    assert path_query == {"session_name": ("path", "")}


def test_append_query_retains_groups_and_moves_replacements_to_end() -> None:
    query = {"session_name": ("new", ""), "cache_buster": "x+y"}
    database = append_query_to_database(
        "md:analytics?session_name=first&threads=2&session_name=&label=a%2Bb"
        "&label=hello+world&bare&empty=&label=%E9%9B%AA",
        query,
    )

    assert database == (
        "md:analytics?threads=2&label=a%2Bb&label=hello+world&label=%E9%9B%AA"
        "&bare=&empty=&session_name=new&session_name=&cache_buster=x%2By"
    )
    assert query == {"session_name": ("new", ""), "cache_buster": "x+y"}


@pytest.mark.parametrize(
    ("database", "query", "expected"),
    [
        (None, {}, None),
        ("md:analytics?bare&x=%2b", {}, "md:analytics?bare&x=%2b"),
        (None, {"label": ""}, "?label="),
        ("md:analytics", {"label": ("one", "two")}, "md:analytics?label=one&label=two"),
        ("md:analytics?", {"label": ""}, "md:analytics?label="),
        ("md:analytics?a=1&b=2&a=3", {"a": []}, "md:analytics?b=2"),
    ],
)
def test_append_query_empty_and_missing_query_cases(
    database: str | None, query: dict[str, Any], expected: str | None
) -> None:
    assert append_query_to_database(database, query) == expected
