# Contributing

Thanks for taking the time to contribute.

## Setup

Requirements: Docker with Compose v2, `make` and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pre-commit install
make up
```

## Before you open a pull request

```bash
make lint       # ruff, ruff format --check, mypy --strict
make coverage   # unit + integration tests with the 90 % branch-coverage gate (Docker required)
make up         # rebuild the stack with your change
make e2e        # end-to-end tests against the running stack
```

- Write the test first: every behaviour change comes with a test that fails without it.
- Keep the public API backward compatible: fields may be added, never removed or renamed.
- A change to a delivery guarantee needs an ADR in `docs/adr/`.
- Commit messages: one imperative subject line (`fix: …`, `feat: …`, `docs: …`).
