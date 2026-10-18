from airflow.sdk import dag, task
import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.ingestion import find_batch, load_batch, archive_batch
from src.raw_to_staging import find_raw_batch, load_batch_to_staging
from src.staging_to_warehouse_audit import (
    find_staging_batches,
    register_batch_run,
    validate_batch,
    load_quarantine,
    load_fact_sales,
    finish_batch_run,
)


@dag(
    dag_id="pos_etl",
    start_date=datetime.datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["pos", "etl"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=1),
    },
)
def pos_etl():  # pos_etl dag itu digambarkan dengan 1 proses penuh
    @task  # digambarkan dengan 1 sub proses
    def ingestion_data_to_raw():
        batches = find_batch()

        if not batches:
            print("No incoming batches data")
            return

        hook = PostgresHook(postgres_conn_id="pos_dwh")
        connection = hook.get_conn()
        try:
            for batch in batches:
                load_batch(connection, batch)
                archive_batch(batch)
        finally:
            connection.close()

        print(f"{len(batches)} batch load to raw succesfully and archived!")

    @task
    def raw_to_staging():
        # dari raw ke staging ini validasi data type dan pengecekan awal kalo batch yang sudah diproses raw sama staging sama
        hook = PostgresHook(postgres_conn_id="pos_dwh")
        connection = hook.get_conn()

        try:
            batch_ids = find_raw_batch(connection)

            if not batch_ids:
                print("No raw batches to proceed")
                return

            for batch_id in batch_ids:
                load_batch_to_staging(connection, batch_id)
        finally:
            connection.close()

    @task
    def staging_validation_to_warehouse_audit():
        # validasi, lalu pisah ke quarantine (invalid) atau warehouse (valid)
        hook = PostgresHook(postgres_conn_id="pos_dwh")
        connection = hook.get_conn()

        try:
            batch_ids = find_staging_batches(connection)

            if not batch_ids:
                print("No staging batches to proceed")
                return

            cursor = connection.cursor()
            try:
                for batch_id in batch_ids:
                    try:
                        register_batch_run(cursor, batch_id)
                        validate_batch(cursor, batch_id)
                        load_quarantine(cursor, batch_id)
                        load_fact_sales(cursor, batch_id)
                        finish_batch_run(cursor, batch_id)
                        connection.commit()
                    except Exception:
                        connection.rollback()
                        raise
            finally:
                cursor.close()
        finally:
            connection.close()

        print(f"{len(batch_ids)} batch processed to warehouse/audit")

    data = ingestion_data_to_raw()
    staging_data = raw_to_staging(data)
    clean_data = staging_validation_to_warehouse_audit(staging_data)


pos_etl()
