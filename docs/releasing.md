# Releasing

1. Create a release branch, update the package version in `pyproject.toml` and
   the editable project entry in `uv.lock`, and move Unreleased changes into a
   dated version section in `CHANGELOG.md`.
2. Open a pull request and wait for the required checks and publishing-workflow
   validation to pass. Merge the release pull request.
3. The publishing workflow detects the version change on `main`, waits for
   `tests_passed`, `pre-commit` and `ty` on that exact commit, builds and
   smoke-tests the distributions, then publishes through the existing PyPI
   trusted publisher.
4. The workflow compares PyPI wheel and sdist hashes with the built artifacts
   and smoke-tests a fresh PyPI wheel with Arrow and pandas. It then creates
   the GitHub release and tag pointing to the tested commit and attaches both
   distributions.

Changes to `pyproject.toml` that keep the same package version do not publish.
Publishing a GitHub release manually remains supported: its tag must match
the package version and its commit must have successful required checks.
Write release titles and notes without semicolons.

To correct the current published release notes, edit its version section in
`CHANGELOG.md` and merge the change through a checked pull request. When the
package version stays unchanged, the workflow updates the existing GitHub
release description from that section.

The workflow uses read-only checkout credentials. Only the PyPI job requests
an OIDC token. Only jobs that create or edit GitHub releases can write
repository contents.

If a job fails, fix the cause and rerun failed jobs. PyPI versions cannot be
overwritten. If publication succeeded but verification or GitHub release
creation failed, rerun only the failed jobs rather than publishing again.
The automatic GitHub release uses `GITHUB_TOKEN`, so its release event does
not start a second publishing run.

See the [Actions publishing runs](https://github.com/leonardovida/duckdb-sqlalchemy/actions/workflows/publish.yaml)
and [PyPI project](https://pypi.org/project/duckdb-sqlalchemy/).
