# OpenBesluitvorming fixtures

**These are synthetic.** They are modelled on the documented response shapes in
https://github.com/ontola/openbesluitvorming/blob/main/API.md (Nov 2026), because the API
could not be reached from the build environment. IDs, titles and URLs are invented.

Run `uv run python scripts/probe.py --save-fixtures` from a machine with internet access.
It writes real responses to `tests/fixtures/openbesluitvorming/real/`. Then compare them with
these files and adjust `pipeline/sources/openbesluitvorming.py` where the real shape differs
(especially the date and title fields inside `payload`, which API.md does not fully document).
