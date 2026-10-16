import os
import csv
from datetime import datetime

INCOMING_DIR = "include/data/pos_exports/incoming"
ARCHIVE_DIR = "include/data/pos_exports/archive"


def find_batches():
    # disini kita mendefinisikan 1 batch = 1 tanggal + 1 outlet, jadi kalo ada 1 tanggal + 10 outlet maka ada 10 batches
    # asumsikan bahwa folder incoming itu terisi setiap harinya dan yang di folder incoming ini akan di load ke raw
    if not os.path.exists(INCOMING_DIR):
        raise ValueError(f"Incoming directory {INCOMING_DIR} does not exist")

    batches = []
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

            batches.append({
                "batch_id": f"{outlet_id}_{date_folder}",
                "outlet_id": outlet_id,
                "batch_date": date_folder,
                "path": date_path,
            })
    
    return sorted(batches, key=lambda b: (b["batch_date"], b["outlet_id"])) 

def read_csv(file_path, wanted_columns):
    #cek file csv yang ada dari path, kolom yang mau dicek apakah ada di dalam file csv yang dimaksud
    with open(file_path, "r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        header = reader.fieldnames or []

        for column in wanted_columns:
            if column not in header:
                raise ValueError(f"Kolom {column}, tidak ada di {file_path}")

        rows = []
        for row in reader:
            rows.append([row[column] for column in wanted_columns])

    return rows


def load_batch(connection, batches):
    loaded_at = datetime.now()
    
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

    for batch in batches:
        batch_id = batch["batch_id"]
        outlet_id = batch["outlet_id"]
        batch_date = batch["batch_date"]
        path = batch["path"]

        print(f"Loading batch: {batch_id}")

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
            cursor.executemany(
                """
                INSERT INTO raw.raw_transactions (
                    batch_id,
                    loaded_at,
                    transaction_id,
                    outlet_id,
                    transaction_time,
                    order_type,
                    total_amount
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        batch_id,
                        loaded_at,
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
                    loaded_at,
                    transaction_id,
                    item_id,
                    product_id,
                    quantity,
                    unit_price,
                    discount_amount,
                    line_total,
                    line_cost
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        batch_id,
                        loaded_at,
                        *row,
                    )
                    for row in items
                ],
            )

            cursor.executemany(
                """
                INSERT INTO raw.raw_payments (
                    batch_id,
                    loaded_at,
                    payment_id,
                    transaction_id,
                    payment_method,
                    paid_amount
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        batch_id,
                        loaded_at,
                        *row,
                    )
                    for row in payments
                ],
            )

            connection.commit()

            print(
                f"Batch {batch_id} berhasil dimuat: "
                f"{len(transactions)} transactions, "
                f"{len(items)} items, "
                f"{len(payments)} payments"
            )

        except Exception:
            connection.rollback()
            print(f"Batch {batch_id} gagal dimuat.")
            raise

        finally:
            cursor.close()

    