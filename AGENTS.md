# Project: my-data-pipeline

## Purpose
- This repo scrapes and normalizes cost-of-living, quality-of-life, and salary data for a dashboard that compares cities for data professionals.

## Stack
- Python 3.12+
- SQLite for local development
- pytest for tests
- Ruff for linting and formatting 

## Conventions
- Use type hints everywhere.
- Prefer explicit imports over wildcard or implicit imports.
- All new modules should have corresponding tests when practical.
- Prefer small, reviewable diffs over broad rewrites.
- Prefer decisions that improve data reliability, joinability, and city-level consistency.

## Workflow
- Explain what you are about to do before doing it.
- Ask before destructive git commands (e.g. reset, force push).
- Ask before deleting files.

### Implementation loop
- Work in small, iterative changes.
- Run tests frequently during development (`make test`) when relevant.
- Do not expand scope beyond the current task without updating TODO.md and confirming direction.

### Review & completion
- When a feature is complete or a milestone is reached, ask to spawn reviewer subagent for a final quality gate.
- Incorporate reviewer feedback before proceeding.
- When well scoped milestone is reached run the below commit process.

### Checks pipeline
- Run `uvx ruff check`
- Run `uvx ruff format --check`
- Run `make test`
- If checks fail, fix issues and re-run before continuing.

### Commit pipeline 
- Run checks pipeline. Ensure it is correct.
- Use `$commit-message` skill to generate a commit message.
- Never commit to main. Always create a branch, or switch to an appropriate branch.

## Planning With `TODO.md`
- `TODO.md` is the shared working plan between the user and the agent.
- For substantial work, ask to write the plan in `TODO.md` before implementation starts.
- Keep tasks concise, concrete, and checkable.
- Update `TODO.md` as work progresses to reflect completion, scope changes, blockers, and relevant next steps.
- Small trivial changes may skip `TODO.md`, but anything multi-step should be tracked there.

## Handling `scratchpad.md`
- `scratchpad.md` is user-owned.
- Do not read, search, index, summarize, or use `scratchpad.md` as context while working in the codebase.
- Ignore `scratchpad.md` completely, whether or not the file exists.

## Execution Notes
- Before editing files, briefly state the intended change.
- If a task grows beyond the original plan, update `TODO.md` before continuing.
- When finishing a task, ensure `TODO.md` reflects the current state and next steps.
