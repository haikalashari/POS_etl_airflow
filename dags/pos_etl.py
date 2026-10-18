from airflow.sdk import dag, task
import datetime 
import src.ingestion import find_batch, read_csv, load_batch, archive_batch
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="pos_etl",
    start_date=datetime.datetime(2026,9,1),
    schedule="@daily",
    catchup=False
    )

def pos_etl(): #pos_etl dag itu digambarkan dengan 1 proses penuh
    @task #digambarkan dengan 1 sub proses
    def ingestion_data_to_raw():
        batches = find_batch()

        if not batches:
            print("No incoming batches data")
            return 
        hook = PostgresHook(postgres_conn_id = "pos_dwh")
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
        #dari raw ke staging ini validasi data type dan pengecekan awal kalo batch yang sudah diproses raw sama staging sama
    @task
    def staging_validation_to_warehouse_audit():
        #disini seharusnya banyak ni validasi karna mau masuk ke quarantine atau ke warehouse

    data = ingestion_data_to_raw()
    staging_data = raw_to_staging(data)
    clean_data = staging_validation_to_warehouse_audit(staging_data)

pos_etl()        