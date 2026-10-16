# STRUCT field processors

## Goal and scope

Run declared SQLAlchemy Python bind and result hooks for STRUCT fields. Preserve
native fields without hooks, ASCII-only case-insensitive matching, SQL NULL,
input dictionaries, nested containers, cache identity and dependency floors.
No UNION conversion, native ENUM DDL registration, retries or new features.

## Candidate comparison

| Responsibility and path | Current evidence and impact | Decision and overlap |
| --- | --- | --- |
| Bulk fallback, `_bulk_insert.py::build_bulk_insert_dataframe` through threshold INSERT | Proposed nested NaN guard was disproved by execution: ordinary native bindings also store nested NaN as NULL. An earlier native-scan probe had not compared the ordinary insert path. | Reject the pandas-only fix. Do not silently redefine native nested NaN semantics. |
| Alembic `DuckDBImpl.prep_table_for_batch`, schema reflection and batch rename callers | Existing real schema/attached-catalog rename and batch tests retain rows, sequence positions, unique constraints and comments, with empty namespace-normalized autogenerate diffs. | No supported defect found. Independent of the previous MAP/Decimal/bulk inspection and current MotherDuck PR200. |
| Retry `_prepare_retry`, `ConnectionWrapper._count_statement` and idempotence callers | Fault injection after native writes preserves earlier rows; commented sequence calls do not replay; ordinary commented reads retry. Alembic/retry/review-contract baseline: 78 passed. | No supported defect found. No retry widening or cleanup refactor. |
| STRUCT `datatypes.py::Struct`, typed INSERT/SELECT and nested ARRAY/MAP callers | Missing field hooks skip decorators, reject Python Enum, return raw JSON/Enum/Numeric values and store `Decimal('1E+5')` as `1.00` on ordinary/pandas inserts. Nine new execution cases: eight fail on baseline, Arrow Decimal control passes. | Select bounded correctness fix. PR198 MAP processing is merged. PR200 MotherDuck paths do not overlap implementation. |

## Plan and progress

1. Read current guidance, docs, configuration, all three schedule records and
   issue discussions. Main `92ef363` advanced to `df09408` when PR200 merged.
2. Compare candidates and prove failures before implementation. Done.
3. Apply dialect-adapted member hooks and add real-file regression coverage.
4. Run focused/full/floor/SQLAlchemy2.1 checks, pre-commit, types and immutable
   published-package reproduction. Review the final diff before delivery.
5. Deliver through a checked PR and verify exact merged workflows. A confirmed
   public wrong-data bug warrants the next maintenance release through the
   established publishing workflow, subject to current version state.

## Discoveries and decisions

- Use ASCII folding rather than Unicode `lower()` because DuckDB distinguishes
  non-ASCII case variants in field names. Preserve caller dictionaries.
- Calling SQLAlchemy ARRAY's shape-only bind processor turns ndarrays into
  lists of NumPy scalars that DuckDB cannot bind. Keep native ndarray binding
  when the ARRAY element has no bind hook. Existing precision tests cover this.
- Native UNION results do not identify the active member. Do not extend this
  patch to UNION without a separate supported conversion contract.
- The old task worktree was removed externally during discovery. No production
  edits were lost. Created a managed replacement and left the canonical checkout
  and all other branches untouched.

## Validation and delivery

- Final tests on exported baseline `df09408`: eight failures, one Arrow Decimal
  control passes. All nine pass with the fix. A fresh immutable PyPI 1.5.6.2
  installation also stores the wrong magnitude and skips field decorators.
- Locked Python3.14 / DuckDB1.5.6 / SQLAlchemy2.0.52: 135 focused passed,
  two remote skips. Full suite: 707 passed, five skips (three remote-data,
  credentialed MotherDuck and the opt-in upstream suite).
- Minimum Python3.10 / DuckDB1.3.0 / SQLAlchemy2.0.0: 46 focused passed,
  including MAP and STRUCT cases; the final ARRAY guard passes 40 focused cases.
  SQLAlchemy2.1.4: 40 focused passed.
- Canonical ty, lock consistency and diff checks pass. Pre-commit required
  formatting only. Wheel/sdist strict Twine checks and a fresh wheel outside
  the source tree pass the original repro and distribution smoke without
  pandas or Arrow. Full local nox and credentialed MotherDuck are not run.
- All hosted stable, floor, platform, optional-dependency, upstream and
  distribution gates passed. Independent review reproduced the ASCII result
  casing issue, verified its fix and found no remaining issue.

## Outcomes

[PR201](https://github.com/leonardovida/duckdb-sqlalchemy/pull/201) merged as
`21e518b6c70f6cc0e4fe285626dd8906ef7ac3a7` after required checks and review.
The tested and merged trees match. The informational DuckDB nightly
`2.0.0.dev2610151406` has the same 12 Arrow/retry failures on the unchanged
baseline and the final patch, with all nine new cases passing. No retry or
unsupported upstream compatibility patch. A separate maintenance release PR
prepares 1.5.6.3 through the established exact-commit checks, PyPI artifact hash
verification and fresh install workflow. The automation continuity record
tracks its terminal publication state and retains detailed baseline logs.
