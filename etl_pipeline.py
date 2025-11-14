#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL Pipeline for Project-Y Members Data
Extracts data from SQL Server, transforms it, and writes summary tables back.
"""

import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv  # Load .env support

# -----------------
# LOAD ENV VARIABLES
# -----------------
load_dotenv()  # Load environment variables from .env

DB_SERVER = os.getenv("SQL_SERVER")
DB_NAME = os.getenv("SQL_DB")
DB_USER = os.getenv("SQL_USER")
DB_PASSWORD = os.getenv("SQL_PASSWORD")
ODBC_DRIVER = os.getenv("ODBC_DRIVER")

CONNECTION_STRING = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}"
    f"?driver={ODBC_DRIVER.replace(' ', '+')}"
)

# -----------------
# LOGGING CONFIG
# -----------------
LOG_FILE = "etl_pipeline.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("etl_pipeline")

# -----------------
# DB CONNECTION
# -----------------
def get_engine(conn_str: str = CONNECTION_STRING) -> Engine:
    try:
        engine = create_engine(conn_str, fast_executemany=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Successfully connected to SQL Server.")
        return engine
    except Exception:
        logger.exception("Failed to create engine or connect to DB.")
        raise

# -----------------
# EXTRACT
# -----------------
def extract_table(engine: Engine, table_name: str) -> pd.DataFrame:
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
        logger.info("Extracted %d rows from %s", len(df), table_name)
        return df
    except Exception:
        logger.exception("Failed to extract table %s", table_name)
        raise

# -----------------
# TRANSFORM HELPERS
# -----------------
def clean_programs(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    expected = ["program_id", "program_name", "program_description", "duration_weeks",
                "start_date", "end_date", "capacity", "is_active"]
    for col in expected:
        if col not in df.columns:
            logger.warning("Column %s missing from Programs; creating with null values.", col)
            df[col] = pd.NA
    df["duration_weeks"] = pd.to_numeric(df.get("duration_weeks", 0), errors="coerce").fillna(0).astype(int)
    df["start_date"] = pd.to_datetime(df.get("start_date"), errors="coerce")
    df["end_date"] = pd.to_datetime(df.get("end_date"), errors="coerce")
    df["capacity"] = pd.to_numeric(df.get("capacity", 0), errors="coerce").fillna(0).astype(int)
    df["is_active"] = pd.to_numeric(df.get("is_active", 0), errors="coerce").fillna(0).astype(int)
    df["duration_days"] = df["duration_weeks"] * 7
    return df

def clean_projects(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    for col in ["due_date", "created_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "week_number" in df.columns:
        df["week_number"] = pd.to_numeric(df["week_number"], errors="coerce").fillna(0).astype(int)
    return df

def clean_progress(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df["completion_percentage"] = pd.to_numeric(df.get("completion_percentage", 0), errors="coerce").fillna(0)
    df["grade"] = pd.to_numeric(df.get("grade", 0), errors="coerce").fillna(0)
    for col in ["start_date", "completion_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).fillna("unknown")
    return df

def clean_team_members(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    if "joined_date" in df.columns:
        df["joined_date"] = pd.to_datetime(df["joined_date"], errors="coerce")
    return df

def clean_teams(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    if "submission_date" in df.columns:
        df["submission_date"] = pd.to_datetime(df["submission_date"], errors="coerce")
    df["score"] = pd.to_numeric(df.get("score", 0), errors="coerce").fillna(0)
    return df

# -----------------
# AGGREGATIONS
# -----------------
def build_program_summary(programs, projects, teams, team_members):
    pr, pj, tm, tmem = programs, projects, teams, team_members
    team_project = tm[["team_id", "project_id", "score"]].copy() if "project_id" in tm.columns else tm[["team_id", "score"]].assign(project_id=pd.NA)
    project_program = pj[["project_id", "program_id"]] if "program_id" in pj.columns else pd.DataFrame(columns=["project_id","program_id"])
    if not project_program.empty:
        team_project = team_project.merge(project_program, on="project_id", how="left")
    if "team_id" in tmem.columns and "member_id" in tmem.columns:
        team_member_counts = tmem.groupby("team_id")["member_id"].nunique().reset_index(name="total_team_members")
        team_project = team_project.merge(team_member_counts, on="team_id", how="left")
    else:
        team_project["total_team_members"] = 0
    if "program_id" in pr.columns and "program_id" in team_project.columns:
        agg = team_project.groupby("program_id").agg(
            total_teams=("team_id","nunique"),
            total_members=("total_team_members","sum"),
            avg_team_score=("score","mean")
        ).reset_index()
    else:
        agg = pd.DataFrame(columns=["program_id","total_teams","total_members","avg_team_score"])
    result = pr.merge(agg, on="program_id", how="left")
    result["total_projects"] = pj.groupby("program_id")["project_id"].nunique().reindex(result["program_id"]).fillna(0).values if "program_id" in pj.columns else 0
    for col in ["total_teams","total_members","avg_team_score","capacity","is_active"]:
        result[col] = pd.to_numeric(result.get(col, 0)).fillna(0)
    result["capacity_sum"] = result["capacity"]
    result["active_program_flag"] = result["is_active"]
    result["report_generated_at"] = datetime.utcnow()
    return result[["program_id","program_name","total_projects","total_teams","total_members",
                   "avg_team_score","capacity_sum","active_program_flag","report_generated_at"]]

def build_team_performance_safe(teams, team_members, progress, members):
    t, tm, p, m = teams, team_members, progress, members if members is not None else pd.DataFrame()
    p["completion_percentage"] = pd.to_numeric(p.get("completion_percentage",0), errors="coerce").fillna(0)
    p["grade"] = pd.to_numeric(p.get("grade",0), errors="coerce").fillna(0)
    if "team_id" in tm.columns and "member_id" in tm.columns:
        team_sizes = tm.groupby("team_id")["member_id"].nunique().reset_index(name="team_size")
    else:
        team_sizes = pd.DataFrame(columns=["team_id","team_size"])
    if not p.empty and "member_id" in p.columns:
        member_progress = p.groupby("member_id").agg(avg_completion=("completion_percentage","mean"), avg_grade=("grade","mean")).reset_index()
    else:
        member_progress = pd.DataFrame(columns=["member_id","avg_completion","avg_grade"])
    if not tm.empty and "team_id" in tm.columns and "member_id" in tm.columns:
        tm_mp = tm.merge(member_progress, on="member_id", how="left")
        tm_agg = tm_mp.groupby("team_id")[["avg_completion","avg_grade"]].mean().reset_index()
    else:
        tm_agg = pd.DataFrame(columns=["team_id","avg_completion","avg_grade"])
    report = t.merge(team_sizes, on="team_id", how="left").merge(tm_agg, on="team_id", how="left")
    report["last_submission_date"] = pd.to_datetime(report.get("submission_date"), errors="coerce") if "submission_date" in report.columns else pd.NaT
    for col in ["team_size","avg_completion","avg_grade"]:
        report[col] = pd.to_numeric(report.get(col,0)).fillna(0)
    report["report_generated_at"] = datetime.utcnow()
    cols = ["team_id","team_name","project_id","team_size","avg_completion","avg_grade","last_submission_date","status","report_generated_at"]
    return report[[c for c in cols if c in report.columns]]

def build_member_progress(progress, members):
    p = progress
    m = members if members is not None else pd.DataFrame()
    name_col = None
    if not m.empty:
        possible_names = [c for c in m.columns if "name" in c.lower() or "full_name" in c.lower()]
        if possible_names:
            name_col = possible_names[0]
    if {"start_date","completion_date"}.issubset(p.columns):
        p["last_update"] = p[["start_date","completion_date"]].max(axis=1)
    if name_col and "member_id" in p.columns:
        p = p.merge(m[["member_id",name_col]], on="member_id", how="left")
    cols = ["member_id", name_col if name_col else "member_id", "course_name","completion_percentage","status","grade","last_update"]
    report = p[[c for c in cols if c in p.columns]].copy()
    if name_col:
        report = report.rename(columns={name_col:"member_name"})
    report["report_generated_at"] = datetime.utcnow()
    return report

# -----------------
# LOAD
# -----------------
def write_table(engine: Engine, df: pd.DataFrame, table_name: str, if_exists="replace"):
    if df.empty:
        logger.warning("No data to write for table %s. Skipping.", table_name)
        return
    try:
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False, method="multi")
        logger.info("Wrote %d rows to %s", len(df), table_name)
    except SQLAlchemyError:
        logger.exception("Failed to write table %s", table_name)
        raise

# -----------------
# MAIN ETL
# -----------------
def main():
    engine = get_engine()

    # Extract
    df_programs = extract_table(engine, "Programs")
    df_projects = extract_table(engine, "Projects")
    df_progress = extract_table(engine, "Progress")
    df_team_members = extract_table(engine, "Team_Members")
    df_teams = extract_table(engine, "Teams")
    try:
        df_members = extract_table(engine, "Members")
    except Exception:
        logger.warning("Members table not found. Proceeding without it.")
        df_members = pd.DataFrame()

    # Transform
    program_summary = build_program_summary(clean_programs(df_programs),
                                            clean_projects(df_projects),
                                            clean_teams(df_teams),
                                            clean_team_members(df_team_members))
    team_perf_safe = build_team_performance_safe(clean_teams(df_teams),
                                                 clean_team_members(df_team_members),
                                                 clean_progress(df_progress),
                                                 df_members)
    member_progress_report = build_member_progress(clean_progress(df_progress), df_members)

    # Load
    write_table(engine, program_summary, "Program_Summary_Report")
    write_table(engine, team_perf_safe, "Team_Performance_Report")
    write_table(engine, member_progress_report, "Member_Progress_Report")

if __name__ == "__main__":
    main()
