from airflow.sdk import dag, task
import datetime 

@dag(
    dag_id="pos_etl",
    start_date=datetime.datetime(2026,9,1),
    schedule="@daily",
    catchup=False
    )

def pos_etl(): #pos_etl dag itu digambarkan dengan 1 proses penuh
    @task #digambarkan dengan 1 sub proses
    def ingestion_data_to_raw():
        #task 1 check the folder, ada ga file yang diinginkan dan sesuai kondisi
        #task 2 read the file, ada ga kolom yang diperlukan dan sesuai kondisi
        #task 3 load the file sesuai dengan table raw di db
    @task
    def raw_to_staging():
        #dari raw ke staging ini validasi data type dan pengecekan awal kalo batch yang sudah diproses raw sama staging sama
    




pos_etl()        