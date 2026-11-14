# FreshCart Pipelines – From Git to Gold

Data Engineering assignment: version control, CI/CD, ELT on Snowflake, an ML pipeline in Mage, and event streaming
with RabbitMQ – built, run, broken on purpose and explained, end to end.

Repository: <https://github.com/Arisex96/freshcart-pipelines>

---

## Contents
1. [Project overview](#1-project-overview)
2. [Assignment objectives](#2-assignment-objectives)
3. [Technologies used](#3-technologies-used)
4. [Prerequisites](#4-prerequisites)
5. [Account creation and initial setup](#5-account-creation-and-initial-setup)
6. [Python environment setup](#6-python-environment-setup)
7. [Git installation and GitHub repository setup](#7-git-installation-and-github-repository-setup)
8. [Repository folder structure](#8-repository-folder-structure)
9. [Installing requirements.txt](#9-installing-requirementstxt)
10. [Snowflake account setup and connection](#10-snowflake-account-setup-and-connection)
11. [Running the FreshCart ELT notebook](#11-running-the-freshcart-elt-notebook)
12. [Bronze, silver, quarantine and gold layers](#12-bronze-silver-quarantine-and-gold-layers)
13. [Mage installation and setup](#13-mage-installation-and-setup)
14. [Running the Iris ML pipeline](#14-running-the-iris-ml-pipeline)
15. [Configuring and running Mage tests](#15-configuring-and-running-mage-tests)
16. [Opening the Mage UI through the Cloudflare tunnel](#16-opening-the-mage-ui-through-the-cloudflare-tunnel)
17. [RabbitMQ installation and configuration](#17-rabbitmq-installation-and-configuration)
18. [Running dispatcher.py](#18-running-dispatcherpy)
19. [Running multiple drivers](#19-running-multiple-drivers)
20. [Testing ACK / NACK behaviour](#20-testing-ack--nack-behaviour)
21. [Git commit and push commands](#21-git-commit-and-push-commands)
22. [GitHub Actions CI/CD configuration](#22-github-actions-cicd-configuration)
23. [Expected outputs and verification](#23-expected-outputs-and-verification)
24. [Common errors and their solutions](#24-common-errors-and-their-solutions)
25. [Screenshots and submission checklist](#25-screenshots-and-submission-checklist)

---

## 1. Project overview

The repository connects four pieces of a small data platform:

```
 laptop ──git push──► GitHub ──► GitHub Actions (pytest on every push)
                          │
   ┌──────────────────────┼─────────────────────────────┐
   ▼                      ▼                             ▼
 Part B: FreshCart ELT   Part C: Mage ML pipeline     Part D: RabbitMQ stream
 sample API data         load_iris → validate_iris    dispatcher → topic exchange
 → bronze → silver       → prepare_data → train_model   rapido-dispatch → drivers
 → dim/ref → gold        → save_results / evaluate     (ACK good, NACK bad)
 (Snowflake)             (@test gates between blocks)
   ▲                                                    │
   └──────────── Bonus: streamed orders land in bronze ◄┘
```

| Folder / file | Part | What it is |
|---|---|---|
| `.github/workflows/ci.yml` | A | CI workflow – installs requirements and runs `pytest` on every push |
| `tests/test_prepare_data.py` | A | pytest test: the Mage `prepare_data` block splits Iris exactly 120 / 30 |
| `elt/freshcart_elt.ipynb` | B | FreshCart ELT notebook with all outputs (B1–B5) |
| `elt/freshcart_stream_bonus.ipynb` | Bonus | RabbitMQ consumer → `write_landing()` → `load_file()` → gold |
| `iris_project/` | C | Mage project (blocks, `metadata.yaml`) + `mage_iris_pipeline.ipynb` with outputs |
| `stream/dispatcher.py`, `stream/driver.py` | D | RabbitMQ food-order stream |

## 2. Assignment objectives

- **Part A – Git and CI/CD:** explain where Git stores work (working folder → staging → local repo → remote) and run a CI workflow that goes green, red, and green again.
- **Part B – ELT:** load raw API data into Snowflake, trace records through bronze/silver/quarantine/gold, add a validation rule, an SCD2 change and a data-quality check, and show the pipeline is idempotent.
- **Part C – ML pipeline:** build an Iris classifier from Mage blocks, pass `max_depth` at run time, and use `@test` functions to stop bad results from being saved.
- **Part D – Streaming:** route food/ride requests with topic-exchange binding patterns and handle bad messages with ACK/NACK.
- **Bonus:** stream orders from RabbitMQ into the FreshCart lakehouse without doubling gold totals.

## 3. Technologies used

| Technology | Used for | Where it runs |
|---|---|---|
| Git + GitHub | version control, remote repository | local PC, github.com |
| GitHub Actions | CI: `pytest` on every push | GitHub's Ubuntu runners |
| Python 3.12 | everything | local PC, Colab, CI |
| Google Colab | running the Snowflake and Mage notebooks | browser |
| Snowflake (trial) | the FreshCart warehouse | cloud |
| Mage (`mage-ai` 0.9.80) | ML pipeline orchestration | Colab (and CI for imports) |
| scikit-learn, pandas | Iris model and data handling | Colab, CI |
| pytest | unit test for `prepare_data` | local PC, CI |
| RabbitMQ + `pika` | message broker and Python client | Docker on the local PC (Part D), Colab (Bonus) |
| cloudflared | public tunnel to the Mage UI running in Colab | Colab |

## 4. Prerequisites

- A Windows / macOS / Linux computer with internet access.
- **Python 3.12** (`python --version` should print `Python 3.12.x`).
- **Git** (`git --version`).
- **Docker Desktop** (only for running RabbitMQ locally in Part D).
- A **Google account** (for Colab), a **GitHub account**, and a **Snowflake trial account**.

## 5. Account creation and initial setup

| Account | Sign-up link | What to do |
|---|---|---|
| GitHub | <https://github.com/signup> | Create an account, then a new **public** repository `freshcart-pipelines` with **no** README (it is pushed from the PC). |
| Snowflake | <https://signup.snowflake.com/> | Choose a trial (Standard edition, any cloud/region). Note your **account identifier** (Snowsight → bottom-left profile → *Account* → e.g. `ABCDEFG-XY12345`) and your **login name**. Remember the password you set. |
| Google Colab | <https://colab.research.google.com/> | Sign in with any Google account. Nothing else to configure. |

> Never put the Snowflake password, account details or a hosted RabbitMQ URL in any file in this repository.

## 6. Python environment setup

Run from the repository root (`freshcart-pipelines/`). A virtual environment keeps this project's packages separate.

```powershell
# Windows (PowerShell) – run from freshcart-pipelines/
python -m venv .venv                       # create the environment in the .venv folder (ignored by Git)
.\.venv\Scripts\Activate.ps1               # activate it (the prompt now starts with (.venv))
```

```bash
# macOS / Linux – run from freshcart-pipelines/
python3.12 -m venv .venv
source .venv/bin/activate
```

## 7. Git installation and GitHub repository setup

1. Install Git from <https://git-scm.com/downloads> and set your identity once:
   ```bash
   git config --global user.name  "Your Name"
   git config --global user.email "you@example.com"
   ```
2. Create the empty GitHub repository (section 5), then in the project folder:
   ```bash
   git init -b main                                                   # start a local repository on branch main
   git remote add origin https://github.com/<your-user>/freshcart-pipelines.git   # remember the GitHub URL as "origin"
   git add .                                                          # stage all files
   git commit -m "Set up repository structure"                        # save a commit locally
   git push -u origin main                                            # upload to GitHub (a sign-in window may open)
   ```

## 8. Repository folder structure

```
freshcart-pipelines/
├── README.md
├── .gitignore              <- ignores .env, *.pkl, *.csv, mage.log, __pycache__/ (+ .venv, tunnel.log, caches)
├── requirements.txt        <- pandas, scikit-learn, pytest, pika, mage-ai==0.9.80
├── pytest.ini              <- puts the repo root on the import path so tests can import iris_project
├── elt/
│   ├── freshcart_elt.ipynb              <- Part B (B1–B5) with outputs
│   └── freshcart_stream_bonus.ipynb     <- Bonus with outputs
├── iris_project/                        <- Mage project (created by `mage init`)
│   ├── mage_iris_pipeline.ipynb         <- Part C notebook with outputs
│   ├── data_loaders/load_iris.py
│   ├── transformers/validate_iris.py    <- my own block (C4)
│   ├── transformers/prepare_data.py
│   ├── transformers/train_model.py
│   ├── transformers/evaluate_model.py   <- evaluation branch (C3)
│   ├── data_exporters/save_results.py
│   └── pipelines/iris_classifier/metadata.yaml
├── stream/
│   ├── dispatcher.py
│   └── driver.py
├── tests/
│   └── test_prepare_data.py
└── .github/workflows/ci.yml
```

`iris_project/` also contains Mage's own scaffold files from `mage init` (`io_config.yaml`, the example
`titanic` pipeline, empty `__init__.py` files); they are left as Mage created them.

## 9. Installing requirements.txt

With the virtual environment active, from the repository root:

```bash
pip install -r requirements.txt     # installs pandas, scikit-learn, pytest, pika and mage-ai
```

- `mage-ai` is pinned to **0.9.80**: unpinned, pip backtracks to an old version that needs `pyarrow 10`, which has no
  Python 3.12 wheel, and the install fails (see section 24).
- On **Windows only**, if pip still tries to build `pyarrow` from source, use
  `pip install --only-binary=pyarrow -r requirements.txt`. (Linux – CI and Colab – does not need this.)

Check it worked:
```bash
pytest          # expected: 1 passed
```

## 10. Snowflake account setup and connection

Nothing has to be created by hand in Snowflake: the notebook's `SQL_00_SETUP` creates the warehouse `FRESHCART_WH`
(X-Small, auto-suspend 60 s), the database `FRESHCART`, the schemas (`bronze`, `silver`, `dim`, `ref`, `gold`,
`quarantine`), the stage and all tables.

The login cell in the notebooks asks for four values with **hidden input** (`getpass`), so nothing is saved in the
notebook or in Git:

| Prompt | What to type |
|---|---|
| Snowflake account identifier | e.g. `ABCDEFG-XY12345` (Snowsight → profile → Account) |
| Snowflake login name | the login name / email you sign in with |
| Snowflake password | your password |
| 6-digit MFA code | the code from your authenticator app, or just press **Enter** if MFA is not enabled |

Browser sign-in (`authenticator="externalbrowser"`) does **not** work in Colab – use the password login above.
Role used: `ACCOUNTADMIN` (fine for a personal trial account).

## 11. Running the FreshCart ELT notebook

1. Open <https://colab.research.google.com> → **File → Upload notebook** → `elt/freshcart_elt.ipynb`.
2. **Runtime → Run all.** Enter the four login values when asked.
3. The notebook first **drops** any old `FRESHCART` database so the B1 numbers come from exactly one run, then:
   - sections 1–10: the original pipeline (Extract → Load → Transform → Check), run twice;
   - **B2**: trace queries for ORD-1003, ORD-1001 and ORD-1005;
   - **B3**: adds the `unsupported_currency` rule to `SQL_10_ORDERS_CLEAN` and shows ORD-1006 before/after;
   - **B4**: predictions table, then adds *Arjun (CUST-02) → Gold from 2026-08-15* and compares;
   - **B5**: the sixth check *orders dated in the future*, made to fail with mock order ORD-9001, then removed.
4. **File → Download → Download .ipynb** to keep the outputs.

`run_pipeline()` does: `write_landing()` (raw `.json.gz` files) → `load_file()` (`PUT` + `COPY INTO` bronze) →
SQL_10 … SQL_40 → `quality_checks()`.

## 12. Bronze, silver, quarantine and gold layers

| Layer | Tables | What it holds |
|---|---|---|
| **Bronze** | `bronze.orders_raw`, `customers_raw`, `fx_raw`, `pincode_raw` | Every API record exactly as received (JSON in a VARIANT column) plus run_id, file and row. Append-only: grows by 8 order rows per run. |
| **Silver** | `silver.orders_checked` → `orders_clean` → `orders_dedup` → `orders_enriched` | Web and mobile fields mapped to one set of columns, cleaned and typed, validated (`reasons` array), de-duplicated (newest `updated_at` wins), then enriched with tier, FX rate and city. |
| **Quarantine** | `quarantine.orders` | Rows that failed validation, with their reasons. Each bad version is stored once (`NOT EXISTS` on source + order_id + updated_at). |
| **Dim / Ref** | `dim.customers_scd2`, `ref.fx_daily`, `ref.pincode_geo`, `ref.tier_multiplier`, `ref.state_region` | Customer tier history (SCD Type 2), daily FX rates with gaps filled, pincode geography, seed tables. |
| **Gold** | `gold.fct_orders` | One row per order, business-ready: amount in INR, loyalty points, region. Written with an idempotent `MERGE` that only updates a row when its `row_hash` changes. |

## 13. Mage installation and setup

Mage runs in Colab (as in the class notebook). `iris_project/mage_iris_pipeline.ipynb` installs it with:

```python
!pip install -q uv
!uv venv -q -p 3.12 /content/mage_env
!uv pip install -q --python /content/mage_env mage-ai scikit-learn
```
then `mage init iris_project` and writes each block with `%%writefile`.

To install Mage on your own PC instead: `pip install -r requirements.txt` (section 9) – used here only so `pytest`
can import the block files.

## 14. Running the Iris ML pipeline

In Colab: **File → Upload notebook** → `iris_project/mage_iris_pipeline.ipynb` → **Runtime → Run all**.
The key command (run from `/content`, the folder that contains `iris_project/`):

```bash
mage run iris_project iris_classifier --test                                   # run all blocks and their @test functions
mage run iris_project iris_classifier --test --runtime-vars '{"max_depth": 1}' # override max_depth for one run
```

`train_model` reads `max_depth` from `kwargs` (default 3, set in `metadata.yaml` under `variables`).

Final block graph (`iris_project/pipelines/iris_classifier/metadata.yaml`):

```
load_iris → validate_iris → prepare_data → train_model ─┬─► save_results
                                                         └─► evaluate_model
```

## 15. Configuring and running Mage tests

Each block file can define `@test` functions; Mage runs them right after the block, on its output, when
`--test` is passed. If a test fails, the block fails and **no downstream block runs**.

| Block | Type | @test |
|---|---|---|
| `load_iris` | data_loader | exactly 150 rows |
| `validate_iris` | transformer | no missing values and exactly 3 classes |
| `prepare_data` | transformer | a `split` column exists |
| `train_model` | transformer | test accuracy ≥ 0.8 |
| `save_results` | data_exporter | – |
| `evaluate_model` | transformer | – |

The blocks import Mage's decorators only if they are missing (`if 'transformer' not in globals(): ...`), so they
can also be imported outside Mage – that is how `tests/test_prepare_data.py` tests `prepare_data` with plain pytest.

## 16. Opening the Mage UI through the Cloudflare tunnel

The last section of the Mage notebook:
1. starts the server: `mage start iris_project` (in the background, output in `mage.log`);
2. downloads `cloudflared` and runs `cloudflared tunnel --url http://localhost:6789`;
3. prints `Open Mage here: https://<random-words>.trycloudflare.com`.

Open the link → **Pipelines → iris_classifier** to see the graph. If a **Sign in** page appears, use Mage's default
local account: email `admin@admin.com`, password `admin`.
The link is public while it runs – close the Colab runtime (**Runtime → Disconnect and delete runtime**) when done.

## 17. RabbitMQ installation and configuration

Simplest local setup (Docker Desktop must be running), from any folder:

```bash
docker run -d --name freshcart-rabbit -p 5672:5672 rabbitmq:3-alpine   # start a broker on localhost:5672
docker logs freshcart-rabbit | grep "Server startup complete"           # wait until this line appears
```

Both scripts connect to `amqp://guest:guest@localhost:5672/` by default. To use another broker (e.g. a CloudAMQP free
plan), set the **`RABBITMQ_URL`** environment variable – never write that URL into a file in the repo:

```powershell
$env:RABBITMQ_URL = "amqps://user:password@host/vhost"     # PowerShell, current window only
```
```bash
export RABBITMQ_URL="amqps://user:password@host/vhost"     # bash
```

Stop / remove the broker afterwards: `docker stop freshcart-rabbit` / `docker rm freshcart-rabbit`.

In Colab (used by the Bonus notebook): `!apt-get install -y rabbitmq-server` then `!service rabbitmq-server start`.

## 18. Running dispatcher.py

From `freshcart-pipelines/stream/` with the virtual environment active:

```bash
python dispatcher.py        # publish a request every 2 seconds until Ctrl+C
python dispatcher.py 60     # stop after 60 requests (= 2 minutes)
```

Each request picks a random city (`bangalore` or `chennai`), zone and type (`ride` / `food`), builds the routing key
`city.zone.type`, and publishes JSON `{"id", "city", "zone", "type", "ts"}` to the **topic exchange
`rapido-dispatch`**. Every 10th request is deliberately bad (no `zone` field) for the ACK/NACK test.

```
sent REQ-0007  key=bangalore.koramangala.food  {"id": "REQ-0007", "city": "bangalore", "zone": "koramangala", ...}
```

## 19. Running multiple drivers

Start the drivers **before** the dispatcher, each in its own terminal, from `freshcart-pipelines/stream/`:

```bash
python driver.py Ravi    "bangalore.koramangala.*"
python driver.py Priya   "bangalore.indiranagar.*"
python driver.py Arjun   "bangalore.*.ride"
python driver.py Meena   "bangalore.*.food"
python driver.py Karthik "chennai.tnagar.food"
python driver.py Control "bangalore.#"
```

Each driver declares a **private queue** (`queue_declare(queue="", exclusive=True)` – the broker names it and deletes
it when the driver stops), binds it to `rapido-dispatch` with its pattern, and prints every request it receives.
`*` matches exactly one word, `#` matches zero or more words. Quote the pattern so the shell does not expand `*`/`#`.

## 20. Testing ACK / NACK behaviour

Run the drivers and `python dispatcher.py 40`. Requests 10, 20, 30 and 40 have no `zone`:

```
[Ravi] REJECTED REQ-0010  key=bangalore.koramangala.ride  missing 'zone' -> NACK
```

- Good message → `basic_ack` (removed from the queue).
- Message without `zone` → `basic_nack(requeue=False)` (discarded, not redelivered).

Check nothing is left behind (while the drivers are still running):

```bash
docker exec freshcart-rabbit rabbitmqctl list_queues name messages_ready messages_unacknowledged
# every amq.gen-... queue shows 0  0
```

## 21. Git commit and push commands

From the repository root:

```bash
git status                       # what changed?
git add <file or folder>         # stage changes for the next commit
git commit -m "Short, meaningful message"   # save a commit in the local repository
git push                         # upload commits to GitHub (this triggers CI)
git log --oneline                # one line per commit
```

A commit is only on your PC until you `git push` – teammates and CI cannot see it before that.

## 22. GitHub Actions CI/CD configuration

`.github/workflows/ci.yml` runs **on every push**:

1. `actions/checkout@v4` – check out the code
2. `actions/setup-python@v5` with Python **3.12**
3. `pip install -r requirements.txt`
4. `pytest` (configured by `pytest.ini`, runs `tests/test_prepare_data.py`)

View runs at **GitHub → repository → Actions**; click a run → **test** → expand **Run pytest** for the log.
A3 demonstration: setting `test_size=0.3` in `prepare_data.py` made CI fail (`assert 105 == 120`), and restoring
`0.2` made it pass again.

## 23. Expected outputs and verification

All values below were produced by actual runs of the code in this repository.

| Check | Expected / observed |
|---|---|
| `pytest` | `1 passed` |
| B1 – bronze rows after one run | 8 (6 web + 2 mobile) |
| B1 – `silver.orders_clean` / quarantine / dedup | 6 / 2 (ORD-1004, ORD-1006) / 4 |
| B1 – gold | `GOLD: 4 orders, revenue ₹7785.00, 126 loyalty points`, 25% rejected |
| B3 – ORD-1006 reasons after the new rule | `bad_email, bad_pincode, unsupported_currency`; gold unchanged |
| B4 – after Arjun → Gold | 2 rows for CUST-02, ORD-1002 Gold 70 points, total 161, 1 gold row re-published |
| B5 – mock future order | `orders dated in the future 1 PROBLEM`; all OK after removal |
| C1 – `mage run … --test` | accuracy 1.0, `Saved 30 predictions to iris_predictions.csv` |
| C2 – max_depth 1 / 2 / 3–5 | 0.633 (test fails, no CSV) / 0.967 / 1.0 (CSV created) |
| C3 – evaluate_model | diagonal confusion matrix (10, 9, 11); accuracy/precision/recall/F1 = 1.0 |
| D – 60 requests, 6 drivers | every request reached exactly the queues predicted by the binding patterns |
| D3 – bad requests | NACKed and discarded; queues empty afterwards |
| Bonus – two identical streaming batches | gold 8 orders, ₹13,870.80, 294 points after **both** batches |

## 24. Common errors and their solutions

| Error | Cause | Fix |
|---|---|---|
| `Failed to build 'pyarrow' … No module named 'pkg_resources'` during `pip install` | unpinned `mage-ai` makes pip backtrack to a version needing `pyarrow 10`, which has no Python 3.12 wheel | keep `mage-ai==0.9.80` pinned; on Windows add `--only-binary=pyarrow` |
| `ModuleNotFoundError: No module named 'iris_project'` in pytest | the repo root is not on the import path | keep `pytest.ini` with `pythonpath = .` and run `pytest` from the repo root |
| `ModuleNotFoundError: No module named 'mage_ai'` in pytest | the block files import Mage's decorators outside Mage | `pip install -r requirements.txt` (includes `mage-ai`) |
| Snowflake login fails in Colab with `externalbrowser` | Colab cannot open a browser on your PC | use the notebook's password login (section 10) |
| Snowflake asks for MFA | MFA is enabled on the user | type the 6-digit code at the MFA prompt |
| Mage UI shows a **Sign in** page | user authentication is on by default | `admin@admin.com` / `admin` |
| `Open Mage here: Not ready yet` | the tunnel needs a few more seconds | run the tunnel cell again |
| `pika.exceptions.AMQPConnectionError` | broker not running / wrong URL | start the Docker broker (section 17) or fix `RABBITMQ_URL` |
| A driver receives nothing | it started after the messages were sent, or the pattern is unquoted | start drivers first; quote patterns like `"bangalore.#"` |
| Bronze row counts keep growing | expected: bronze is append-only | duplicates are removed in silver; gold totals stay the same |
| `iris_predictions.csv` missing after a run | `train_model`'s accuracy test failed (e.g. `max_depth 1`) | expected behaviour of the test gate |

## 25. Screenshots and submission checklist

**Screenshots (report appendix)**
- [x] A3: green CI run, red CI run (`test_size=0.3`), fixed run
- [x] B1–B5: pipeline outputs, ORD-1006 before/after, B4 actuals, B5 PROBLEM output
- [x] C: `mage run` output, C2 table, confusion matrix and metrics, Mage UI graph with the branch (Cloudflare tunnel)
- [ ] D: terminals showing dispatcher + drivers and a `REJECTED … NACK` line
- [x] Bonus: streaming batch output and before / after-batch-1 / after-batch-2 totals

**Submission**
- [x] GitHub repository link (public)
- [x] `elt/freshcart_elt.ipynb` with all outputs visible
- [x] `iris_project/` and `iris_project/mage_iris_pipeline.ipynb` with outputs
- [x] `stream/dispatcher.py`, `stream/driver.py`
- [ ] Report `RollNo_Name_Pipelines.pdf` (≤ 8 pages + screenshot appendix) with the repo URL, all answers A1–D3,
      the integration diagram and the reflection
