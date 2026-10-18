import os
import csv
import shutil
from datetime import datetime

INCOMING_DIR = "include/data/pos_exports/incoming"
ARCHIVE_DIR = "include/data/pos_exports/archive"


def find_batch():
    # disini kita mendefinisikan 1 batch = 1 tanggal + 1 outlet, jadi kalo ada 1 tanggal + 10 outlet maka ada 10 batch
    # asumsikan bahwa folder incoming itu terisi setiap harinya dan yang di folder incoming ini akan di load ke raw
    if not os.path.exists(INCOMING_DIR):
        raise ValueError(f"Incoming directory {INCOMING_DIR} does not exist")

    batch = []
    required_files = [
        "transactions.csv",
        "transaction_items.csv",
        "payments.csv"
    ] 
    
    for outlet_folder in os.listdir(INCOMING_DIR):
        if not outlet_folder.startswith("outlet_"):
            continue
        outlet_path = os.path.join(INCOMING_DIR, outlet_folder)
        if not os.path.isdir(outlet_path):
            continue
        outlet_id = outlet_folder.removeprefix("outlet_")
        
        for date_folder in os.listdir(outlet_path):
            try:
                parsed = datetime.strptime(date_folder, "%Y-%m-%d")
            except ValueError:
                continue
            if parsed.strftime("%Y-%m-%d") != date_folder:
                continue
            date_path = os.path.join(outlet_path, date_folder)

            all_files_exist = True
            for file in required_files:
                if not os.path.isfile(os.path.join(date_path, file)):
                    all_files_exist = False
            if not all_files_exist:
                continue

            batch.append({
                "batch_id": f"{outlet_id}_{date_folder}",
                "outlet_id": outlet_id,
                "batch_date": date_folder,
                "path": date_path,
            })
    
    return sorted(batch, key=lambda b: (b["batch_date"], b["outlet_id"])) 

def read_csv(file_path, wanted_columns):
    #cek file csv yang ada dari path, kolom yang mau dicek apakah ada di dalam file csv yang dimaksud
    with open(file_path, "r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        header = reader.fieldnames or []

        for column in wanted_columns:
            if column not in header:
                raise ValueError(f"Coulm {column}, does not exist in {file_path}")

        rows = []
        for row in reader:
            rows.append([row[column] for column in wanted_columns])

    return rows


def load_batch(connection, batch):
    # 1 kali fungsi dipanggil = 1 batch (1 outlet + 1 tanggal). Loop-nya ada di task DAG.
    batch_id = batch["batch_id"]
    outlet_id = batch["outlet_id"]
    path = batch["path"]

    transaction_columns = [
        "transaction_id",
        "transaction_time",
        "order_type",
        "total_amount",
    ]

    item_columns = [
        "transaction_id",
        "item_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_total",
        "line_cost",
    ]

    payment_columns = [
        "payment_id",
        "transaction_id",
        "payment_method",
        "paid_amount",
    ]

    print(f"Loading batch: {batch_id}")

    # Baca semua CSV dulu. Kalau ada file rusak, error muncul di sini
    # sebelum database disentuh sama sekali.
    transactions = read_csv(
        os.path.join(path, "transactions.csv"),
        transaction_columns,
    )

    items = read_csv(
        os.path.join(path, "transaction_items.csv"),
        item_columns,
    )

    payments = read_csv(
        os.path.join(path, "payments.csv"),
        payment_columns,
    )

    cursor = connection.cursor()

    try:
        # Hapus data lama batch ini, supaya batch yang dimuat ulang tidak jadi ganda
        cursor.execute(
            "DELETE FROM raw.raw_transactions WHERE batch_id = %s", (batch_id,)
        )
        cursor.execute(
            "DELETE FROM raw.raw_transaction_items WHERE batch_id = %s", (batch_id,)
        )
        cursor.execute(
            "DELETE FROM raw.raw_payments WHERE batch_id = %s", (batch_id,)
        )

        cursor.executemany(
            """
            INSERT INTO raw.raw_transactions (
                batch_id,
                transaction_id,
                outlet_id,
                transaction_time,
                order_type,
                total_amount
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    batch_id,
                    row[0],
                    outlet_id,
                    row[1],
                    row[2],
                    row[3],
                )
                for row in transactions
            ],
        )

        cursor.executemany(
            """
            INSERT INTO raw.raw_transaction_items (
                batch_id,
                transaction_id,
                item_id,
                product_id,
                quantity,
                unit_price,
                discount_amount,
                line_total,
                line_cost
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(batch_id, *row) for row in items],
        )

        cursor.executemany(
            """
            INSERT INTO raw.raw_payments (
                batch_id,
                payment_id,
                transaction_id,
                payment_method,
                paid_amount
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            [(batch_id, *row) for row in payments],
        )

        connection.commit()

        print(
            f"Batch {batch_id} load success: "
            f"{len(transactions)} transactions, "
            f"{len(items)} items, "
            f"{len(payments)} payments"
        )

    except Exception:
        connection.rollback()
        print(f"Batch {batch_id} fail to load.")
        raise

    finally:
        cursor.close()

def archive_batch(batch):
    #pindahin file path dari batch ke archive sesuai dengan pola path sebelumnya
    #dibuat idempotent: aman dipanggil ulang, tidak error kalau archive sudah ada
    source_path = batch["path"]
    outlet_id = batch["outlet_id"]
    batch_date = batch["batch_date"]

    archive_dir = os.path.join(ARCHIVE_DIR, f"outlet_{outlet_id}")
    archive_path = os.path.join(archive_dir, batch_date)

    if not os.path.exists(source_path):
        print(f"Batch {batch['batch_id']} sudah tidak ada di incoming, skip archive")
        return

    os.makedirs(archive_dir, exist_ok=True)

    if os.path.exists(archive_path):
        # sudah pernah di-archive. jangan gagal dan jangan hapus data:
        # simpan sebagai duplikat bertimestamp supaya tidak menimpa archive lama.
        suffix = datetime.now().strftime("%Y%m%d%H%M%S")
        archive_path = f"{archive_path}__dup{suffix}"
        print(f"Archive sudah ada untuk {batch['batch_id']}, disimpan ke {archive_path}")

    shutil.move(source_path, archive_path)
    print(f"Batch {batch['batch_id']} archived successfully")

