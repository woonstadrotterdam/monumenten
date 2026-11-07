# monumenten

- Checks: `uv run pytest` en `uv run pre-commit run --all-files`. `tests/test_client.py` bevraagt de echte API's van RCE en Kadaster: een 503 of `ClientConnectorError` daar is een bronstoring, geen bug.
- Commit met `uv run git commit`: de pre-commit-hook vindt `pre-commit` alleen in de venv.
- Schrijf PR-beschrijvingen, commits en GitHub-reacties in helder Nederlands. Commits en PR-titels volgen [Conventional Commits](https://www.conventionalcommits.org/nl/v1.0.0/), bijvoorbeeld `fix(kadaster): …`; het type in de PR-titel bepaalt het label en daarmee de rubriek in de release notes (`.github/workflows/labeler.yml`).
- Noem een branch `<type>/<korte-omschrijving>` met hetzelfde type, en hernoem een branch met een tool-prefix zoals `claude/` vóór de eerste push.

## Agent skills

### Issue tracker

GitHub Issues van woonstadrotterdam/monumenten, via `gh`. Zie `docs/agents/issue-tracker.md`.

### Triage labels

De vijf standaardlabels (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). Zie `docs/agents/triage-labels.md`.

### Domain docs

Eén context: `GLOSSARY.md` en `docs/adr/` in de root; gebruik de begrippen uit de woordenlijst. Zie `docs/agents/domain.md`.
