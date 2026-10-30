import os
import random
import time
from datetime import datetime, timezone

import pymysql

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    get_current_context,
)
from airflow.providers.standard.sensors.python import PythonSensor


def mysql_connection():
    return pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
        charset="utf8mb4",
        autocommit=True,
    )


# Етап 1. Створення таблиці результатів.
def create_table():
    conn = mysql_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS medal_counts (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    medal_type VARCHAR(10) NOT NULL,
                    `count` INT NOT NULL,
                    created_at DATETIME(6) NOT NULL
                )
            """)
        print("Table medal_counts is ready")
    finally:
        conn.close()


# Етап 2. Випадковий вибір типу медалі.
def select_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Selected medal: {medal}")
    return medal


# Етап 3. Вибір гілки DAG через XCom.
def choose_branch():
    context = get_current_context()
    medal = context["ti"].xcom_pull(task_ids="select_medal")
    branch = f"count_{medal.lower()}"
    print(f"Selected branch: {branch}")
    return branch


# Етап 4. Підрахунок медалей і запис результату в MySQL.
def count_medals(medal):
    conn = mysql_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM athlete_event_results
                WHERE medal = %s
                """,
                (medal,),
            )
            medal_count = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO medal_counts
                    (medal_type, `count`, created_at)
                VALUES (%s, %s, UTC_TIMESTAMP(6))
                """,
                (medal, medal_count),
            )

        print(f"{medal}: {medal_count} medals saved")
    finally:
        conn.close()


# Етап 5. Затримка з можливістю тестування 2 і 35 секунд.
def delay_execution():
    context = get_current_context()
    conf = getattr(context.get("dag_run"), "conf", None) or {}
    seconds = int(conf.get("delay_seconds", 2))

    if not 0 <= seconds <= 60:
        raise ValueError("delay_seconds must be between 0 and 60")

    print(f"Sleeping for {seconds} seconds")
    time.sleep(seconds)


# Етап 6. Сенсор перевіряє вік останнього запису.
def check_recent_record():
    conn = mysql_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT TIMESTAMPDIFF(
                    MICROSECOND,
                    MAX(created_at),
                    UTC_TIMESTAMP(6)
                ) / 1000000
                FROM medal_counts
            """)
            age = cursor.fetchone()[0]
    finally:
        conn.close()

    if age is None:
        print("No records found")
        return False

    age = float(age)
    print(f"Latest record age: {age:.2f} seconds")

    return 0 <= age <= 30


with DAG(
    dag_id="olympic_medal_dag",
    description="GoIT HW07: Olympic medals branching and freshness sensor",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["goit", "hw07", "mysql"],
) as dag:

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=select_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_by_medal",
        python_callable=choose_branch,
    )

    bronze_task = PythonOperator(
        task_id="count_bronze",
        python_callable=count_medals,
        op_kwargs={"medal": "Bronze"},
    )

    silver_task = PythonOperator(
        task_id="count_silver",
        python_callable=count_medals,
        op_kwargs={"medal": "Silver"},
    )

    gold_task = PythonOperator(
        task_id="count_gold",
        python_callable=count_medals,
        op_kwargs={"medal": "Gold"},
    )

    delay_task = PythonOperator(
        task_id="delay_execution",
        python_callable=delay_execution,
        trigger_rule="none_failed_min_one_success",
    )

    freshness_sensor = PythonSensor(
        task_id="check_recent_record",
        python_callable=check_recent_record,
        poke_interval=1,
        timeout=5,
        mode="poke",
    )

    create_table_task >> select_medal_task >> branch_task

    branch_task >> [bronze_task, silver_task, gold_task]

    [bronze_task, silver_task, gold_task] >> delay_task

    delay_task >> freshness_sensor
