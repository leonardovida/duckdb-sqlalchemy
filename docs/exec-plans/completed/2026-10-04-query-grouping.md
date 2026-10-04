# Standard database query grouping

Inspected main `97a854a0786b76fababe68c3c357af4d26c4382a`. No unfinished
refactor. PR #181 changes snapshot docs/example/tests and remains separate.
Canonical checkout is clean and preserved.

## Candidate comparison

| Responsibility and callers | Maintenance benefit and scope | Evidence and compatibility risk | Decision |
| --- | --- | --- | --- |
| DBAPI description and buffered rows: `CursorWrapper.description`, fetch methods, `ConnectionWrapper._buffer_pending_result` | Common snapshot installation already has `_store_buffered_result`; no remaining duplicated ownership rule | Different fetch timing, pending result lifecycle, native readers and DML count contracts need real execution tests | Reject: PR #182 already addressed the useful duplication and October 2 fixes protect distinct failure flows |
| Reflection: `get_multi_pk_constraint`, foreign keys, unique and check constraints | Lookup/grouping already centralized; remaining dictionary assembly reflects distinct SQLAlchemy contracts | Attached catalogs, search paths, views, temp shadows, nested types and missing tables require broad evidence | Reject: another wrapper would duplicate existing ownership and overlap recent maintenance |
| Query parsing: `MotherDuckURL` and `append_query_to_database`, used by engine helpers and dialect connect preparation | Replace two hand-written repeated-value grouping loops with the existing standard-library `parse_qs` implementation | Preserve blank/bare values, repeated-value order, decoding, key order, overrides, scalar/tuple shape and caller mappings | Select: one duplicated rule with two real callers, no new abstraction, dependency or public API change |
| Older config dispatch: `apply_config` / `_render_config_value`, used during dialect connect | Possible replacement of mutable type dispatch by SQLAlchemy inference | Bool-before-int/subclass order, PathLike/Decimal handling and mutable `TYPES` differ from inferred literal types | Reject: no demonstrated equivalent library contract; low benefit with compatibility risk. This older dispatch was inspected beyond recently changed execution paths |

## Invariant and short plan

Decoded embedded query values retain blanks and are grouped by first key
appearance, retaining each key's value order. URL construction then collapses
one value to a scalar and multiple values to a tuple. Database-string merging
keeps lists for `urlencode(doseq=True)`, excludes overridden groups before
adding explicit keys, and preserves its no-query fast paths.

1. Run existing helper/core/connect tests and representative baseline cases.
   Add API-level characterization tests before source changes.
2. Replace only the two grouping loops with `parse_qs(...,
   keep_blank_values=True)`. Keep partitioning, alias warnings, precedence,
   credential key scanning, exports and signatures in place.
3. Run focused and full locked tests, minimum-version focused tests, all-file
   pre-commit and canonical ty. Inspect the diff, deliver a PR through stable
   hosted gates, then verify the exact merged SHA and main workflows.

No performance claim. No remote SQL execution is changed. Version and
changelog stay untouched: the current publishing workflow runs for changes
to those files and this source-only refactor must not trigger publication.

## Validation

Before source edits, the existing helper/core suite
passed 179 tests with one SQLAlchemy 2.1-only skip on the locked Python 3.14,
DuckDB 1.5.5, SQLAlchemy 2.0.52 environment. Seven executable query probes
showed equivalence of grouped values and key order with standard-library
`parse_qs`, including empty, repeated, malformed UTF-8 and semicolon cases.

The first draft also asserted that SQLAlchemy's `make_url(str(url))` retained
blank values. Three baseline cases disproved that assumption: SQLAlchemy's
parser drops blanks. Removed that unrelated assertion before refactoring.
The dialect's own embedded-query parsing keeps blanks and remains the invariant
under test. No attempt to change SQLAlchemy's parsing behavior is in scope.

The corrected characterization/connect baseline passed all 59 cases before
editing source. The same cases pass after the refactor. Final focused tests
passed 238 with one SQLAlchemy 2.1-only skip. Full locked tests passed 611 with
six skips (three remote-data, credentialed MotherDuck, SQLAlchemy 2.1-only
context and opt-in upstream suite). Minimum Python 3.10.19 / DuckDB 1.3.0 /
SQLAlchemy 2.0.0 focused tests passed 237 with two expected skips (Quack floor
and SQLAlchemy 2.1-only context). Canonical ty and lock consistency passed.
Ruff corrected one blank line between test import groups on the first
pre-commit pass. No source behavior changed during formatting.
All-file pre-commit passed on rerun. All three remote-data tests passed
separately. The reviewed diff is limited to the two grouping loops, their
import, API characterization tests and this plan.

Credentialed MotherDuck, full local nox and local package builds were not run:
this change alters URL parsing only, with no packaging/public import changes.
Hosted stable matrix and distribution gates are required before merge.
