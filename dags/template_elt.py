from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os

TMP_PATH = "/tmp/sales_extract.csv"

def extract_to_csv(**context):
    """Извлечение инкрементальных данных из MSSQL и сохранение в CSV"""
    mssql = MsSqlHook(mssql_conn_id="mssql_source")

    last_loaded = Variable.get("last_sales_loaded_at", default_var="2000-01-01 00:00:00")
    sql = f"""
        SELECT id, product, amount, updated_at
        FROM dbo.sales
        WHERE updated_at > '{last_loaded}'
    """

    df = mssql.get_pandas_df(sql)
    rowcount = len(df)

    if rowcount == 0:
        raise ValueError("⚠️ Нет новых строк для загрузки. Прерываем DAG.")

    df.to_csv(TMP_PATH, index=False)
    print(f"✅ Extracted {rowcount} rows to {TMP_PATH}")

    # передаём максимальную дату обновления через XCom
    context["ti"].xcom_push(key="max_updated_at", value=str(df["updated_at"].max()))


def load_csv_to_postgres(**context):
    """Двухфазная загрузка CSV: staging_tmp → staging"""
    pg = PostgresHook(postgres_conn_id="postgres_target")
    conn = pg.get_conn()
    cur = conn.cursor()

    # создаём временную таблицу (если нет)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_sales_tmp (
            id INT,
            product TEXT,
            amount NUMERIC(10,2),
            updated_at TIMESTAMP
        );
    """)
    conn.commit()

    # очищаем временную таблицу
    cur.execute("TRUNCATE TABLE stg_sales_tmp;")
    conn.commit()

    # копируем CSV → stg_sales_tmp
    with open(TMP_PATH, "r", encoding="utf-8") as f:
        cur.copy_expert("COPY stg_sales_tmp FROM STDIN WITH CSV HEADER", f)

    # атомарная замена staging (двухфазно)
    cur.execute("""
        BEGIN;
        TRUNCATE TABLE stg_sales;
        INSERT INTO stg_sales SELECT * FROM stg_sales_tmp;
        COMMIT;
    """)
    conn.commit()
    cur.close()
    print("✅ Loaded data into stg_sales via stg_sales_tmp")


def transform_staging(**context):
    """MERGE данных из staging в основную таблицу dim_sales"""
    pg = PostgresHook(postgres_conn_id="postgres_target")
    pg.run("""
        INSERT INTO dim_sales (id, product, amount, updated_at)
        SELECT id, product, amount, updated_at
        FROM stg_sales s
        ON CONFLICT (id) DO UPDATE
        SET product = EXCLUDED.product,
            amount = EXCLUDED.amount,
            updated_at = EXCLUDED.updated_at
        WHERE EXCLUDED.updated_at > dim_sales.updated_at;
    """)
    print("✅ Merged data into dim_sales")


def update_last_loaded(**context):
    """Обновление Variable после успешного завершения трансформации"""
    max_date = context["ti"].xcom_pull(task_ids="extract_to_csv", key="max_updated_at")
    Variable.set("last_sales_loaded_at", max_date)
    print(f"✅ Updated last_loaded timestamp: {max_date}")


with DAG(
    dag_id="mssql_to_postgres_csv_safe",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",  # ежедневно в 2:00
    catchup=False,
    tags=["elt", "mssql", "postgres", "incremental"],
) as dag:

    extract = PythonOperator(
        task_id="extract_to_csv",
        python_callable=extract_to_csv,
    )

    load = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
    )

    transform = PythonOperator(
        task_id="transform_staging",
        python_callable=transform_staging,
    )

    update_var = PythonOperator(
        task_id="update_last_loaded",
        python_callable=update_last_loaded,
    )

    extract >> load >> transform >> update_var
