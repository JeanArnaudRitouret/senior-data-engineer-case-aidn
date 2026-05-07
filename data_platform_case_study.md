# Data Platform Engineer — Take-Home Case Study

## The task
Move data reliably from a source system to a destination. Treat the source system as an OLTP system backed by a relational database, and the target system as an OLAP system.

Pick your own tooling.If you use a programming laguage, use Python.

## Setup

As setup to the practical part, we simulate a source system by running Postgres in a container. We'll use DuckDB as the destination. Attached you'll find a python script to generate text data, and a `docker compose` file to stand up the database. Run these in order

You are free to modify the script to get it to run, if for some reason it does not work on your system.
If you feel you are spending an inordinate amout of time on setup, you may make your own scenario. The important part is that you get to show how you solve common data engineering problems.

You don't have to set up orchestration or scheduling. A manually runnable script is fine

```bash
python generate_data.py
docker compose up -d
# postgres://postgres:dev@localhost:5432/postgres
```

Init scripts only run on a fresh data directory. To re-seed:

```bash
docker compose down -v && docker compose up -d
```

To simulate a second load, run your own `UPDATE` / `INSERT` / `DELETE` against Postgres. Document what you did.

## On AI tools
Use them. We expect it. We care about your judgment on what to accept, override, or rewrite — AI may write the code, but you own it. 

## Submission
- Git repo (GitHub / GitLab / Azure DevOps) or zip
- Top-level `README.md` with run instructions

Make and document reasonable assumptions, and don't hesitate to reach out if blocked.
