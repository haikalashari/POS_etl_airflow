#buat function yang identifikasi ada berapa batch untuk diproses dari raw ke staging 
#buat function load dimana per batchnya akan dilakukan looping lewat dag 
#reconsilasi apakah batch yang dimasukin ke staging itu sama dengan batch yang ada saat find batch

TABLE_PAIRS = [
    ("raw.raw_transactions", "staging.staging_transactions"),
    ("raw.raw_transaction_items", "staging.staging_transaction_items"),
    ("raw.raw_payments", "staging.staging_payments"),
]

#TRIM kalo ada spasi lebih
#NULLIF kalo datanya kosong
#pg_input_is_valid pakai postgres 16, true jika data type bisa diubah ke tipe tersebut


INSERT_TRANSACTIONS_SQL = """
INSERT INTO staging.staging_transactions (
    batch_id, transaction_id, outlet_id, transaction_time, transaction_date,
    order_type, total_amount, error_codes
)
SELECT
    batch_id,
    NULLIF(TRIM(transaction_id), ''),
    NULLIF(TRIM(outlet_id), ''),
    CASE WHEN pg_input_is_valid(TRIM(transaction_time), 'timestamp')
         THEN TRIM(transaction_time)::timestamp END,
    CASE WHEN pg_input_is_valid(TRIM(transaction_time), 'timestamp')
         THEN TRIM(transaction_time)::timestamp::date END,
    NULLIF(TRIM(order_type), ''),
    CASE WHEN pg_input_is_valid(TRIM(total_amount), 'numeric(14,2)')
         THEN TRIM(total_amount)::numeric(14,2) END,
    CASE WHEN NOT pg_input_is_valid(TRIM(transaction_time), 'timestamp')
           OR NOT pg_input_is_valid(TRIM(total_amount), 'numeric(14,2)')
         THEN 'R8' END
FROM raw.raw_transactions
WHERE batch_id = %s
"""
INSERT_ITEMS_SQL = """
INSERT INTO staging.staging_transaction_items (
    batch_id, transaction_id, item_id, product_id, quantity, unit_price,
    discount_amount, line_total, line_cost, error_codes
)
SELECT
    batch_id,
    NULLIF(TRIM(transaction_id), ''),
    NULLIF(TRIM(item_id), ''),
    NULLIF(TRIM(product_id), ''),
    CASE WHEN pg_input_is_valid(TRIM(quantity), 'integer')
         THEN TRIM(quantity)::integer END,
    CASE WHEN pg_input_is_valid(TRIM(unit_price), 'numeric(14,2)')
         THEN TRIM(unit_price)::numeric(14,2) END,
    CASE WHEN pg_input_is_valid(TRIM(discount_amount), 'numeric(14,2)')
         THEN TRIM(discount_amount)::numeric(14,2) END,
    CASE WHEN pg_input_is_valid(TRIM(line_total), 'numeric(14,2)')
         THEN TRIM(line_total)::numeric(14,2) END,
    CASE WHEN pg_input_is_valid(TRIM(line_cost), 'numeric(14,2)')
         THEN TRIM(line_cost)::numeric(14,2) END,
    CASE WHEN NOT pg_input_is_valid(TRIM(quantity), 'integer')
           OR NOT pg_input_is_valid(TRIM(unit_price), 'numeric(14,2)')
           OR NOT pg_input_is_valid(TRIM(discount_amount), 'numeric(14,2)')
           OR NOT pg_input_is_valid(TRIM(line_total), 'numeric(14,2)')
           OR NOT pg_input_is_valid(TRIM(line_cost), 'numeric(14,2)')
         THEN 'R8' END
FROM raw.raw_transaction_items
WHERE batch_id = %s
"""
 
INSERT_PAYMENTS_SQL = """
INSERT INTO staging.staging_payments (
    batch_id, payment_id, transaction_id, payment_method, paid_amount, error_codes
)
SELECT
    batch_id,
    NULLIF(TRIM(payment_id), ''),
    NULLIF(TRIM(transaction_id), ''),
    NULLIF(TRIM(payment_method), ''),
    CASE WHEN pg_input_is_valid(TRIM(paid_amount), 'numeric(14,2)')
         THEN TRIM(paid_amount)::numeric(14,2) END,
    CASE WHEN NOT pg_input_is_valid(TRIM(paid_amount), 'numeric(14,2)')
         THEN 'R8' END
FROM raw.raw_payments
WHERE batch_id = %s
"""

#function find_raw_batch lakuin fetch pakai sql untuk query select ke raw apakah ada batchnya
def find_raw_batch(connection):
    with connection.cursor() as cursor:
        # cek 1 table aja gamasalah seharusnya karena di raw pasti ke load semua jadi gaperlu cek 3 3nya
        cursor.execute(
        """
        SELECT DISTINCT r.batch_id
        FROM raw.raw_transactions r
        WHERE NOT EXIST (
            SELECT 1
            FROM staging.staging_transactions s
            WHERE s.batch_id = r.batch_id
        )
        ORDER BY r.batch_id
        """
        )
        rows = cursor.fetchall()
    return (row[0] for row in rows)

def count_batch_rows(cursor, table, batch_id):
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE batch_id = %s", (batch_id,))
    return cursor.fetchone()[0]

def load_batch_to_staging(connection, batch_id):
    print(f"Staging batch: {batch_id}")

    cursor = connection.cursor()

    try:
        # hapus staging lama batch ini, supaya diproses ulang tidak jadi ganda
        for _, staging_table in TABLE_PAIRS:
            cursor.execute(
                f"DELETE FROM {staging_table} WHERE batch_id = %s", (batch_id,)
            )

        # execute SQL
        cursor.execute(INSERT_TRANSACTIONS_SQL, (batch_id,))
        cursor.execute(INSERT_ITEMS_SQL, (batch_id,))
        cursor.execute(INSERT_PAYMENTS_SQL, (batch_id,))

        # rekonsiliasi jumlah baris raw dan staging
        for raw_table, staging_table in TABLE_PAIRS:
            raw_count = count_batch_rows(cursor, raw_table, batch_id)
            staging_count = count_batch_rows(cursor, staging_table, batch_id)

            if raw_count != staging_count:
                raise ValueError(f"jumlah baris tidak sesuai untuk {batch_id}" f"{raw_table}={raw_count}, {staging_table}={staging_count}")

        connection.commit()
        print(f"batch {batch_id} berhasil di load ke staging")
    
    except Exception:
        connection.rollback()
        print(f"batch {batch_id} tidak dapat di load ke staging")
        raise

    finally:
        connection.close()