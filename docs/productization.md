# Productization & Runbook

## Purpose
Make the system feel like a **platform**, not a notebook.

---

## One-Command Experience

Target:
ingest → aggregate → analyze → memo

Achieved via:
`run_end_to_end`

---

## Runbook

A new user should be able to:
1. generate data
2. run pipelines
3. inspect Iceberg tables
4. read memo

No manual debugging allowed.

---

## UX Philosophy

- CLI first
- UI-lite optional
- Artifacts > dashboards

---

## Definition of Done

- Fresh clone works
- Docs are complete
- Outputs are inspectable