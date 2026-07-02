# 🧭 Build a Sales Data Pipeline — Complete Guide
### Airflow + Docker + Snowflake, from absolute zero

**You have never used Airflow, Docker, or Snowflake. This guide assumes exactly that.**

Everything is here in this one document — what each tool is, *why* we use it, and every
step to build a working, realistic data pipeline. Follow it top to bottom. Don't skip.
Where you must type your own value, it looks like `THIS_IN_CAPITALS`. After important
steps there's a **✅ checkpoint** — always confirm it before moving on.

**Time:** about 90 minutes the first time. Take breaks whenever you like.

---

## 📖 Contents

- [PART A — Understand the tools (read first, no computer needed)](#a)
  - [A1. The big picture: what is a data pipeline?](#a1)
  - [A2. What is Airflow?](#a2)
  - [A3. What is Docker?](#a3)
  - [A4. What is Snowflake?](#a4)
  - [A5. How the three work together](#a5)
  - [A6. The 5 words that are all of Airflow](#a6)
  - [A7. What you'll build today](#a7)
- [PART B — Run Airflow with Docker](#b)
  - [B1. (Windows only) Set up Ubuntu/WSL](#b1)
  - [B2. Install Docker Desktop](#b2)
  - [B3. Get the project folder & understand its files](#b3)
  - [B4. Download the Docker Compose file (+ one edit)](#b4)
  - [B5. Create the .env file](#b5)
  - [B6. Build and start Airflow](#b6)
  - [B7. Open the dashboard](#b7)
- [PART C — Set up Snowflake](#c)
  - [C1. Create a free Snowflake account](#c1)
  - [C2. Run the setup SQL](#c2)
  - [C3. Connect Airflow to Snowflake](#c3)
- [PART D — Build & run the pipeline](#d)
  - [D1. Understand the pipeline](#d1)
  - [D2. Upload the first data file](#d2)
  - [D3. Run it and see your results](#d3)
  - [D4. See the "incremental" magic](#d4)
- [PART E — Recap, problems & next steps](#e)

---

<a name="a"></a>
# PART A — Understand the tools
*(Read this like a short story. It makes every step afterward make sense.)*

<a name="a1"></a>
## A1. The big picture: what is a data pipeline?

Every day, businesses get new data — sales, sign-ups, payments — usually as **files**.
Someone has to take those files, put them into a database, tidy them up, and turn them
into **reports** people can use.

Doing that by hand every day is slow and error-prone. A **data pipeline** is an
automated assembly line that does it for you: *take the data → load it → transform it →
produce reports*, on a schedule, reliably.

That's what you're building today. Three tools each play one role:

- **Airflow** — the *manager* that runs the steps in order, on time.
- **Docker** — the *clean box* that Airflow runs inside, so the setup is tidy and works
  the same on any computer.
- **Snowflake** — the *warehouse* where the data actually lives and gets processed.

Let's meet each one.

<a name="a2"></a>
## A2. What is Airflow?

**What it is:** Apache Airflow is a free tool that **runs a list of tasks in a set order,
on a schedule, automatically** — and shows you what happened on a web dashboard.

**An analogy:** think of a **kitchen manager**. They don't cook every dish themselves —
they make sure each step happens in the right order ("chop before you fry"), at the right
time ("start the roast at 5pm"), and they notice immediately if something burns.

**Why we need it:** real data jobs are repeating, multi-step, and must not break silently.
Airflow gives you four things:
- **Order** — step 3 never runs before step 2 finishes.
- **Timing** — "run every morning at 6am" (a built-in alarm clock).
- **Reliability** — if a step fails, it can retry and alert you.
- **Visibility** — a dashboard shows every run: what worked, what failed, and why.

**How it works:** you describe your pipeline in a small **Python file**. Airflow reads it
and runs it. A pipeline is called a **DAG** (just read "DAG" as "pipeline"); each step is
a **task**.

**Where it runs:** on your computer now (for learning); on servers or cloud in a company.

<a name="a3"></a>
## A3. What is Docker?

This is the part people find fuzzy, so let's be really clear.

**What it is:** Docker is a tool that packs a program **plus everything it needs to run**
(the right Python version, libraries, settings) into a sealed box called a **container**.
That container then runs **the same way on any computer**.

**An analogy:** a **shipping container**. Before shipping containers, loading a boat was
chaos — every item packed differently. The container standardized it: seal your goods in
a box, and any ship, truck, or crane handles it identically. Docker does that for
software. Another way to picture it: a **lunchbox** that already contains the meal, the
cutlery, and the napkin — you just open and eat; nothing missing.

**Why we use it (this matters for a real project):**
- **"It works on my machine" disappears.** Everyone who runs your project gets the exact
  same environment.
- **It's clean.** Nothing gets messily installed onto your computer; it all lives inside
  containers you can start and stop.
- **Airflow is actually several programs** (a web dashboard, a scheduler, a database…).
  Docker starts them all together with **one command**, correctly wired up.
- **It matches industry.** This is how data teams actually run Airflow.

**How it works — three words you'll see:**
- **Image** = a *recipe/snapshot* of a sealed box (e.g. "Airflow with the Snowflake
  add-on"). Built from a file called a **Dockerfile**.
- **Container** = a *running copy* of an image (the actual box, live and working).
- **Docker Compose** = a file (`docker-compose.yaml`) that describes **several
  containers** and how they connect, so you can start them all at once with
  `docker compose up`.

**Where it runs:** containers run on your computer through **Docker Desktop** (the app you
install in B2). In a company, the same containers run on cloud servers.

> 🔑 **The key point:** Docker doesn't *replace* Airflow. Docker is just the clean,
> organized *box* that Airflow runs inside. You'll still use the exact same Airflow
> dashboard at `localhost:8080`.

<a name="a4"></a>
## A4. What is Snowflake?

**What it is:** Snowflake is a **cloud data warehouse** — a big, fast database that lives
on the internet. You store data in it (in **tables**, like spreadsheets) and ask it
questions using **SQL**.

**An analogy:** a giant, well-organized **filing cabinet** in the cloud that can also do
math on its contents instantly.

**Why we use it:** it stores lots of data cheaply, runs big calculations fast, and needs
**no installation** — it's all in your web browser.

**How it fits with Airflow:**
> **Airflow is the worker. Snowflake is the filing cabinet.**
> Airflow doesn't hold your data — its steps *tell Snowflake what to do* ("load this
> file," "add up these sales").

**Where it runs:** entirely in the cloud; you use it through a website called Snowsight.

<a name="a5"></a>
## A5. How the three work together

Here's the whole system in one sentence:

> **Docker** runs **Airflow** on your computer; **Airflow** tells **Snowflake** (in the
> cloud) to load and transform your sales data.

Picture it:

```
   YOUR COMPUTER                                        THE CLOUD
  ┌───────────────────────────────────┐
  │  Docker                            │
  │   ┌─────────────────────────────┐ │
  │   │ Airflow                     │ │        ┌──────────────────┐
  │   │  • web dashboard (8080)     │ │  ───▶  │    Snowflake     │
  │   │  • scheduler + workers      │ │        │  (your tables)   │
  │   │  • Postgres (its memory)    │ │        └──────────────────┘
  │   └─────────────────────────────┘ │
  └───────────────────────────────────┘
```

<a name="a6"></a>
## A6. The 5 words that are all of Airflow

Learn these five and you understand Airflow. Everything else is detail.

| Word | Plain meaning | Everyday comparison |
|------|---------------|---------------------|
| **Pipeline** (real name *DAG*) | A list of steps run in order | A recipe |
| **Task** | One single step | One line of the recipe |
| **Dependencies** | Which step comes before which | "Boil water *before* the pasta" |
| **Schedule** | When it runs | An alarm clock |
| **Dashboard** | Where you watch it | A control panel |

<a name="a7"></a>
## A7. What you'll build today

A realistic **daily sales pipeline**. Files of sales orders arrive; your pipeline loads
them, transforms them, and produces report tables — adding only new data each day
(**incremental**).

**Data teams keep data in "layers." You'll build all three:**
1. **`raw_sales`** — the *landing zone*: files land here untouched (the raw truth).
2. *(transform in between — the pipeline calculates revenue and groups the data)*
3. **Three final report tables** — your polished output:
   - `summary_by_region` — orders + revenue per region
   - `summary_by_product` — orders + revenue per product
   - `summary_by_day_region` — orders + revenue per day, per region

**The pipeline (one DAG, its steps run in order):**

```
  Files arrive in a Snowflake "stage" (a drop-box for files)
                     │
                     ▼
  Step 1  LOAD      →  copy the files into raw_sales          (COPY INTO)
  Step 2  CAPTURE   →  grab only the brand-new rows           (a "bookmark")
  Step 3  SUMMARIZE →  update the 3 report tables (in parallel) (MERGE)
  Step 4  CHECK     →  make sure the reports have data
```

**Your data:** 3 files, `daily_sales_2024_01_01/02/03.csv`, **12,000 orders total**
(4,000 per day). Columns: `order_id, order_date, product, region, quantity, unit_price`.

Okay — now let's build it. 🚀

---

<a name="b"></a>
# PART B — Run Airflow with Docker

<a name="b1"></a>
## B1. (Windows only) Set up Ubuntu/WSL
*(On Mac or Linux? Skip to B2.)*

Docker on Windows works best with **WSL** (a small, free Linux inside Windows). Set it up
once:

1. Click **Start**, type `PowerShell`, right-click **Windows PowerShell**, choose
   **Run as administrator**, click **Yes**.
2. Type this and press Enter:
   ```powershell
   wsl --install
   ```
3. **Restart your PC** (required).
4. After restart, an **Ubuntu** window opens; create a **username** and **password**
   when asked (⚠️ the password won't show as you type — that's normal). ✍️ Write it down.

✅ You now have WSL/Ubuntu ready. Docker Desktop (next) will use it automatically.

<a name="b2"></a>
## B2. Install Docker Desktop

**What you're doing:** installing the app that runs containers on your computer.

1. Download **Docker Desktop**: https://www.docker.com/products/docker-desktop/
2. Install it.
   - **Windows:** during install, keep **"Use WSL 2"** ticked.
3. **Open Docker Desktop and leave it running.** You'll see a **whale icon** in your
   taskbar (Windows) or menu bar (Mac) — that means Docker is on.
4. Give Docker enough memory: Docker Desktop → **Settings → Resources** → set memory to
   **at least 4 GB (8 GB is better)**. Airflow needs it.

✅ Docker Desktop is installed and running (whale icon visible).

Quick test — open a terminal (on Windows, use the **Ubuntu** app) and type:
```bash
docker --version
```
You should see a version number. 🎉

<a name="b3"></a>
## B3. Get the project folder & understand its files

Download and unzip this project. You'll have a folder like this:

```
sales-pipeline-project/
├── dags/
│   └── daily_sales_pipeline.py    ← the pipeline (Python)
├── data/
│   ├── daily_sales_2024_01_01.csv ← the sales files (upload these to Snowflake)
│   ├── daily_sales_2024_01_02.csv
│   └── daily_sales_2024_01_03.csv
├── Dockerfile                     ← the "recipe" for our Airflow box
├── requirements.txt               ← extra packages to add (Snowflake support)
├── .env.example                   ← a small settings file (you'll copy it to .env)
├── setup.sql                      ← run once in Snowflake
└── COMPLETE_GUIDE.md              ← this document
```

**What each Docker-related file is (plain words):**
- **`Dockerfile`** — the *recipe*. It says "start from the official Airflow box, then add
  the Snowflake packages." Docker reads it to build our custom **image**.
- **`requirements.txt`** — the *shopping list* of extra Python packages (here: the
  Snowflake add-ons).
- **`docker-compose.yaml`** — (you download it in B4) the file that defines **all** the
  Airflow services and starts them together.
- **`.env`** — a tiny settings file Docker reads automatically.

Open a terminal, and `cd` into the folder (on Windows, do this in the **Ubuntu** app):
```bash
cd path/to/sales-pipeline-project
```

Create the folders Airflow uses for logs and plugins:
```bash
mkdir -p logs plugins config
```

<a name="b4"></a>
## B4. Download the Docker Compose file (+ one edit)

The `docker-compose.yaml` file defines every Airflow service (dashboard, scheduler,
database…). We download the **official** one so it's guaranteed correct:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml'
```

✅ You now have `docker-compose.yaml` in your folder.

**Now one small edit** so it uses *our* `Dockerfile` (the one with Snowflake support).
Open `docker-compose.yaml` in a text editor and find these two lines near the top:

```yaml
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.2.2}
  # build: .
```

Change them to (comment the first, un-comment the second):

```yaml
  # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.2.2}
  build: .
```

Save the file.

*(Optional, for a cleaner dashboard: in the same file find*
`AIRFLOW__CORE__LOAD_EXAMPLES: 'true'` *and change* `'true'` *to* `'false'` *to hide the
built-in example pipelines.)*

<a name="b5"></a>
## B5. Create the .env file

This sets file permissions so containers and you share files cleanly:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

*(If that command doesn't work, just make a copy of `.env.example` named `.env`.)*

<a name="b6"></a>
## B6. Build and start Airflow

**1) Build our custom image** (only needed once, or after changing `requirements.txt`):
```bash
docker compose build
```
This downloads the Airflow box and adds Snowflake support. Give it a few minutes.

**2) Set up Airflow's database** (run once):
```bash
docker compose up airflow-init
```

**3) Start everything:**
```bash
docker compose up -d
```
(`-d` means "run in the background.") The first start takes 1–3 minutes.

**Check the containers are healthy:**
```bash
docker compose ps
```
✅ You should see services like `apiserver`, `scheduler`, `postgres` marked **healthy** or
**running**.

<a name="b7"></a>
## B7. Open the dashboard

Open your web browser to:
```
http://localhost:8080
```
Log in with username **`airflow`** and password **`airflow`**.

✅ **You're looking at Airflow, running in Docker.** 🎉 (You'll see your
`daily_sales_pipeline` in the list — we'll run it in Part D.)

> 🛑 **To stop later:** `docker compose down` (keeps your data).
> **To start again:** `docker compose up -d`. Keep Docker Desktop running while you work.

---

<a name="c"></a>
# PART C — Set up Snowflake

<a name="c1"></a>
## C1. Create a free Snowflake account

1. Go to **https://signup.snowflake.com/**
2. Fill in the form (any cloud, any region is fine). Choose the **free trial**.
3. Check your email → click the **activation link**.
4. Create a **username** and **password**. ✍️ **Write these down.**
5. You'll land on Snowflake's website, **Snowsight**.

**Get your account identifier** (needed to connect Airflow):
- Look at your browser's address bar: `https://app.snowflake.com/abcdefg/xy12345/...`
- Your identifier is usually **`abcdefg-xy12345`** (the two parts joined with a dash).
- ✍️ Write it down as `YOUR_ACCOUNT_IDENTIFIER`.

<a name="c2"></a>
## C2. Run the setup SQL

1. In Snowsight: **Projects → Worksheets → `+`** (opens a blank SQL page).
2. Open `setup.sql`, copy **all** of it, paste it into the worksheet, click **▶ Run**.

✅ You should see `Setup complete - you are ready`.

**What you just created (plain words):** a drop-box for files (`sales_stage`), a landing
table (`raw_sales`), a "new rows" bookmark (`new_sales_stream`), a small holding table
(`new_sales_batch`), and the 3 report tables. The pipeline uses all of these for you.

<a name="c3"></a>
## C3. Connect Airflow to Snowflake

Airflow needs your Snowflake login, saved once as a **connection**.

1. In the Airflow dashboard, click **Admin → Connections** in the top menu.
2. Click **`+`** (add).
3. Fill in these boxes (leave others blank):

| Box | What to type |
|-----|--------------|
| Connection Id | `snowflake_default` |
| Connection Type | choose **Snowflake** |
| Login | your Snowflake **username** |
| Password | your Snowflake **password** |
| Account | `YOUR_ACCOUNT_IDENTIFIER` (e.g. `abcdefg-xy12345`) |
| Warehouse | `COMPUTE_WH` |
| Database | `AIRFLOW_DB` |
| Schema | `PUBLIC` |

4. Click **Save**.

✅ `snowflake_default` now appears in your connections list. (The pipeline looks for
exactly that name.)

---

<a name="d"></a>
# PART D — Build & run the pipeline

<a name="d1"></a>
## D1. Understand the pipeline

Your pipeline is already in `dags/daily_sales_pipeline.py`. You don't need to write it —
but here's what each step does, so it's not a mystery:

| Step | Task name | Plain-English job | Snowflake tool |
|------|-----------|-------------------|----------------|
| 1 | `load_raw` | Load staged files into the landing table | `COPY INTO` |
| 2 | `capture_new_rows` | Take only the brand-new rows | stream + `INSERT` |
| 3a | `update_summary_by_region` | Add new data to region totals | `MERGE` |
| 3b | `update_summary_by_product` | Add new data to product totals | `MERGE` |
| 3c | `update_summary_by_day_region` | Add new data to day+region totals | `MERGE` |
| 4 | `check_final_tables` | Confirm the outputs have rows | `SELECT COUNT` |

The order is: `load_raw → capture_new_rows → (3a, 3b, 3c together) → check_final_tables`.

- **`COPY INTO`** = Snowflake's fast, standard command for loading files from a stage.
- **stream** = the "bookmark" that knows which rows are new (this is what makes it
  incremental).
- **`MERGE`** = "if this group already exists, add to it; if it's new, create it."

<a name="d2"></a>
## D2. Upload the first data file

Start with **Day 1** so you can watch the totals grow later.

1. In Snowsight: **Data → Databases → AIRFLOW_DB → PUBLIC → Stages → SALES_STAGE**.
2. Click **`+ Files`** (top right).
3. Upload **only** `daily_sales_2024_01_01.csv`. Click **Upload**.

✅ The file now appears inside the stage. *(In a real company, another system drops files
here automatically — here you do it by hand to see how it works.)*

<a name="d3"></a>
## D3. Run it and see your results

1. In the Airflow dashboard, find **`daily_sales_pipeline`** and turn it **ON** (toggle).
2. Click its name → **Graph** (to see the steps as boxes).
3. Click **▶ Trigger** (top right) → confirm.
4. Watch the boxes turn **dark green**, in order. The 3 summary steps light up together.
5. Click `check_final_tables` → **Logs** to see the row counts.

**See your report tables** — in Snowsight, run:
```sql
USE DATABASE AIRFLOW_DB; USE SCHEMA PUBLIC;

SELECT * FROM summary_by_region      ORDER BY total_revenue DESC;
SELECT * FROM summary_by_product     ORDER BY total_revenue DESC;
SELECT * FROM summary_by_day_region  ORDER BY order_date, region;
```

✅ **You'll see Day-1 totals in all three tables.** 🎉🎉 You just ran a real pipeline
that loaded 4,000 orders and built reports.

<a name="d4"></a>
## D4. See the "incremental" magic

1. In Snowflake, go back to the `SALES_STAGE` stage (D2 path).
2. Upload **`daily_sales_2024_01_02.csv`** (Day 2).
3. In Airflow, trigger **`daily_sales_pipeline`** again.
   - It loads only the new file (Day 1 is skipped automatically).
   - It grabs only Day-2's rows (the bookmark remembers Day 1 is done).
   - It adds Day-2's numbers on top of the existing totals.
4. Check again:
```sql
SELECT * FROM summary_by_day_region ORDER BY order_date, region;
SELECT * FROM summary_by_region     ORDER BY total_revenue DESC;
```

✅ **You now see both Day 1 and Day 2**, and the region/product totals grew by *exactly*
Day-2's amounts — Day 1 was never counted twice. **That is incremental processing.** 🎉

5. Repeat once more with **`daily_sales_2024_01_03.csv`** (Day 3).

---

<a name="e"></a>
# PART E — Recap, problems & next steps

## 🎉 What you accomplished

- Ran **Airflow inside Docker** — a clean, organized, industry-style setup.
- Connected Airflow to **Snowflake**.
- Built a real pipeline: files land in a stage → load to a raw table → transform → 3
  report tables → check.
- Made it **incremental** — each day only adds new data.

## 🧠 The ideas you actually used

| Idea | Where you saw it |
|------|------------------|
| **Docker image / container** | the box Airflow runs in (`docker compose up`) |
| **Pipeline (DAG)** | `daily_sales_pipeline` |
| **Task** | each box: `load_raw`, `capture_new_rows`, etc. |
| **Dependencies** | the order the boxes ran in |
| **Airflow + Snowflake** | every step told Snowflake what to do |

## 😟 If something didn't work

Click the **red box** in the dashboard → **Logs** → read the bottom. Common issues:

| What you see | Likely cause | Fix |
|--------------|-------------|-----|
| Dashboard keeps restarting | Docker needs more memory | Docker Desktop → Settings → Resources → 4–8 GB |
| `port 8080 already in use` | Something else uses 8080 | Stop it, or change the port in `docker-compose.yaml` |
| Changed `requirements.txt`, no effect | Image not rebuilt | `docker compose build` then `docker compose up -d` |
| `COPY INTO` loaded 0 rows | File not uploaded to the stage | Redo D2; confirm the file shows in `SALES_STAGE` |
| `object does not exist` | `setup.sql` not run / wrong database | Re-run C2; check connection Database = `AIRFLOW_DB` |
| Connection / auth error | Wrong Snowflake details | Fix `snowflake_default` in Admin → Connections |
| `docker: command not found` | Docker Desktop not running | Start Docker Desktop (whale icon), retry |

## ▶️ Where to go next

- Change `schedule=None` to `schedule="@daily"` in the DAG so it runs itself each morning.
- Add an email/Slack alert if the check step finds an empty table.
- Put the whole folder in **git** so anyone can run `docker compose up` and get the same
  environment — that's real team workflow.

**You did it.** You built a professional-shaped data pipeline — Airflow in Docker,
loading and transforming real data in Snowflake, incrementally. That's genuinely what the
job looks like. Be proud. 🙂
