# FreshCart Pipelines – From Git to Gold

Data Engineering assignment: version control, ELT, ML pipelines and event streaming, end to end.

This repository contains four pieces of a small data platform:

| Folder | Part | What it is |
|---|---|---|
| `.github/workflows/` | A | CI workflow – runs `pytest` on every push |
| `elt/` | B | FreshCart ELT notebook – raw data → Snowflake bronze → silver → gold |
| `iris_project/` | C | Mage project – Iris classifier pipeline with `@test` quality gates |
| `stream/` | D | RabbitMQ food-order stream – `dispatcher.py` and `driver.py` |
| `tests/` | A | pytest test for the Mage `prepare_data` block |

> This README is being filled in part by part as the assignment progresses.

## Repository structure

```
freshcart-pipelines/
├── README.md
├── .gitignore          <- ignores .env, *.pkl, *.csv, mage.log, __pycache__/
├── requirements.txt
├── elt/                <- FreshCart notebook (Part B)
├── iris_project/       <- Mage project (Part C)
├── stream/             <- dispatcher and driver scripts (Part D)
├── tests/
│   └── test_prepare_data.py
└── .github/workflows/ci.yml
```

## Secrets

No passwords, tokens or connection URLs are stored in this repository.
Snowflake passwords are typed in at run time (`getpass`), and anything stored locally goes in a `.env` file, which `.gitignore` excludes.
