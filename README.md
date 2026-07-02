# 📊 Daily Sales Data Pipeline — Airflow + Docker + Snowflake

A beginner-friendly data engineering project that automatically loads daily sales files,
cleans and summarizes them, and produces report tables — all on its own.

> 👋 **New to this? Start here.** This page explains, in plain English, what the tools
> are and what this project does. For the full step-by-step build instructions (with
> screenshots), open **`COMPLETE_GUIDE.docx`** in this repo.

---

## 🤔 What is this, in plain English?

Every day a business gets new sales data as **files**. Someone has to load those files
into a database, tidy them up, and turn them into **reports**. Doing that by hand is slow
and easy to get wrong.

This project does it **automatically**: drop in a file → it loads, transforms, and updates
the reports by itself. That automated "assembly line" is called a **data pipeline**.

---

## 🧩 The three tools (and what each one does)

| Tool | What it is | Simple comparison |
|------|-----------|-------------------|
| **Airflow** | Runs the steps in the right order, on a schedule, automatically | A **kitchen manager** who makes sure each step happens at the right time |
| **Docker** | A clean, sealed "box" that Airflow runs inside, so it works the same on any computer | A **shipping container** — pack it once, it runs anywhere |
| **Snowflake** | A cloud database where the data actually lives and gets processed | A giant **filing cabinet** in the cloud that can also do the math |

**In one sentence:** *Docker* runs *Airflow* on a computer, and *Airflow* tells
*Snowflake* to load and transform the sales data.

---

## ⚙️ What does the pipeline actually do?

It runs four steps, in order:

```
  Sales files arrive in Snowflake
              │
              ▼
  1. LOAD       →  copy the files into a raw table
  2. CAPTURE    →  pick out only the brand-new rows
  3. SUMMARIZE  →  update 3 report tables (revenue by region, product, and day)
  4. CHECK      →  make sure the reports have data
```

It's **incremental** — each day it only processes the *new* data, never re-counting what
it already did. That's exactly how real data pipelines work.

**The reports it produces:**
- 💰 Total sales **per region**
- 📦 Total sales **per product**
- 📅 Total sales **per day, per region**

---

## 📁 What's in this repo

| File / folder | What it is |
|---------------|-----------|
| `COMPLETE_GUIDE.docx` | 📖 The full step-by-step guide (start here to build it) |
| `dags/` | The pipeline code (the "recipe" Airflow follows) |
| `data/` | Sample sales files — 12,000 orders across 3 days |
| `setup.sql` | Sets up the tables in Snowflake (run once) |
| `Dockerfile` | Recipe for the Airflow "box" |
| `docker-compose.yaml` | Defines and starts all the pieces together |
| `requirements.txt` | Extra packages the pipeline needs |

---

## 🚀 How to run it (the very short version)

1. Install **Docker Desktop** and start it.
2. In the project folder, run: `docker compose up -d`
3. Open **http://localhost:8080** (login: `airflow` / `airflow`).
4. Set up a free **Snowflake** account and connect it (see the guide).
5. Upload a sales file and trigger the pipeline. Watch the reports appear. 🎉

📖 **Full details, with every step explained, are in `COMPLETE_GUIDE.docx`.**

---

## 🧠 Why this project matters

This is the real shape of professional data engineering: **files land → load to a raw
table → transform → produce reports**, running automatically and incrementally. Small,
but genuinely how the job works.
