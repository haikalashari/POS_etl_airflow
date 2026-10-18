from datetime import datetime

# sesuai dengan aturan validasi di awal pas membuat pos generate, ada beberapa hal yang perlu divalidasi
# DUPLICATE_TRANSACTION
# INVALID_PRODUCT_ID
# INVALID_QUANTITY
# PAYMENT_MISMATCH
# sisanya akan dipikirkan pas code

# kita akan find batch dulu kaya tadi tapi dari staging
def find_staging_batches(connection):
    with connection.cursor() as cursor:
        #cek table dari staging
        cursor.execute(
            """
            SELECT DISTINCT batch_id
            FROM staging.staging_transactions s 
            WHERE NOT EXISTS (
                SELECT 1
                FROM audit.audit_batch_runs a
                WHERE a.batch_id = s.batch_id AND a.status='SUCCESS'
            )
            ORDER BY s.batch_id
            """
        )
        rows = cursor.fetchall()
    return [row[0] for row in rows]

# setelah batch ketemu kita update batch run
def register_batch_run(cursor, batch_id):
    #the idea is register batch run dari batch yang sudah ada dari rows
    #keep the commit process by dag aja jadi ga bingung 
    try:
        outlet_id, batch_date = batch_id.split("_", 1)
    except ValueError:
        raise ValueError(f"Format batch_id tidak valid: {batch_id}")
    try:
        datetime.strptime(batch_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Tanggal pada batch_id tidak valid: {batch_id}")

    cursor.execute(
        """
        DELETE FROM warehouse.fact_sales WHERE batch_id = %s
        """, (batch_id,),
    )
    cursor.execute(
        """
        DELETE FROM audit.audit_quarantine_records WHERE batch_id = %s
        """, (batch_id,),
    )

    cursor.execute(
        """
        INSERT INTO audit.audit_batch_runs (batch_id, outlet_id, batch_date, status, started_at)
        VALUES (%s, %s, %s::date, 'RUNNING', NOW())
        ON CONFLICT (batch_id) DO UPDATE SET
            outlet_id = EXCLUDED.outlet_id,
            batch_date = EXCLUDED.batch_date,
            status = 'RUNNING',
            rows_loaded = NULL,
            rows_quarantined = NULL,
            started_at = NOW(),
            finished_at = NULL
        """,
        (batch_id, outlet_id, batch_date),
    )
    print(f"Batch run registered: {batch_id}")


# validasi dilakukan disini sesuai dengan aturan
def validate_batch(cursor, batch_id):
    # baris tanpa transaction_id tidak bisa divalidasi / dikarantina (kolom transaction_id di quarantine NOT NULL)
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT transaction_id FROM staging.staging_transactions WHERE batch_id = %(b)s
            UNION ALL
            SELECT transaction_id FROM staging.staging_transaction_items WHERE batch_id = %(b)s
            UNION ALL
            SELECT transaction_id FROM staging.staging_payments WHERE batch_id = %(b)s
        ) x
        WHERE transaction_id IS NULL
        """,
        {"b": batch_id},
    )
    if cursor.fetchone()[0] > 0:
        raise ValueError(f"Batch {batch_id} punya baris tanpa transaction_id")

    # snapshot R8 dari raw_to_staging SEBELUM error_codes ditimpa,
    # supaya retry tidak menambahkan R8 palsu ke baris yang sudah punya kode validasi
    cursor.execute("DROP TABLE IF EXISTS tmp_r8")
    cursor.execute(
        """
        CREATE TEMP TABLE tmp_r8 ON COMMIT DROP AS
        SELECT transaction_id, 'transactions' AS source_table
        FROM staging.staging_transactions
        WHERE batch_id = %(b)s AND error_codes IS NOT NULL
        UNION ALL
        SELECT transaction_id, 'transaction_items'
        FROM staging.staging_transaction_items
        WHERE batch_id = %(b)s AND error_codes IS NOT NULL
        UNION ALL
        SELECT transaction_id, 'payments'
        FROM staging.staging_payments
        WHERE batch_id = %(b)s AND error_codes IS NOT NULL
        """,
        {"b": batch_id},
    )

    cursor.execute(
        """
        WITH bad AS (
            -- R1 DUPLICATE_TRANSACTION
            SELECT transaction_id, 'transactions' AS source_table, 'R1' AS error_code
            FROM staging.staging_transactions
            WHERE batch_id = %(b)s AND transaction_id IS NOT NULL
            GROUP BY transaction_id
            HAVING COUNT(*) > 1

            UNION ALL
            -- R2 INVALID_PRODUCT_ID
            SELECT i.transaction_id, 'transaction_items', 'R2'
            FROM staging.staging_transaction_items i
            LEFT JOIN warehouse.dim_product p ON p.product_id = i.product_id
            WHERE i.batch_id = %(b)s AND p.product_key IS NULL

            UNION ALL
            -- R3 INVALID_QUANTITY
            SELECT i.transaction_id, 'transaction_items', 'R3'
            FROM staging.staging_transaction_items i
            WHERE i.batch_id = %(b)s AND (i.quantity IS NULL OR i.quantity <= 0)

            UNION ALL
            -- R5 ORPHAN_ITEM
            SELECT i.transaction_id, 'transaction_items', 'R5'
            FROM staging.staging_transaction_items i
            WHERE i.batch_id = %(b)s
                AND NOT EXISTS (
                    SELECT 1 FROM staging.staging_transactions t
                    WHERE t.batch_id = i.batch_id AND t.transaction_id = i.transaction_id
                )

            UNION ALL
            -- R7 UNKNOWN_OUTLET
            SELECT t.transaction_id, 'transactions', 'R7'
            FROM staging.staging_transactions t
            LEFT JOIN warehouse.dim_outlet o ON o.outlet_id = t.outlet_id
            WHERE t.batch_id = %(b)s AND o.outlet_key IS NULL

            UNION ALL
            -- R6 MISSING_PAYMENT (tidak ada baris pembayaran sama sekali)
            SELECT t.transaction_id, 'transactions', 'R6'
            FROM staging.staging_transactions t
            WHERE t.batch_id = %(b)s
                AND NOT EXISTS (
                    SELECT 1 FROM staging.staging_payments p
                    WHERE p.batch_id = t.batch_id AND p.transaction_id = t.transaction_id
                )

            UNION ALL
            -- R4 PAYMENT_MISMATCH (ada pembayaran tapi jumlahnya tidak cocok, toleransi 0.01)
            SELECT t.transaction_id, 'transactions', 'R4'
            FROM staging.staging_transactions t
            JOIN (
                SELECT transaction_id, SUM(paid_amount) AS total_paid
                FROM staging.staging_payments
                WHERE batch_id = %(b)s
                GROUP BY transaction_id
            ) p ON p.transaction_id = t.transaction_id
            WHERE t.batch_id = %(b)s
                AND t.total_amount IS NOT NULL
                AND ABS(t.total_amount - COALESCE(p.total_paid, 0)) > 0.01

            UNION ALL
            -- R8 INVALID_TYPE (dari snapshot raw_to_staging, tahan retry)
            SELECT transaction_id, source_table, 'R8'
            FROM tmp_r8
        ),
        codes AS (
            SELECT transaction_id,
                    source_table,
                    STRING_AGG(DISTINCT error_code, ',' ORDER BY error_code) AS codes
            FROM bad
            WHERE transaction_id IS NOT NULL
            GROUP BY transaction_id, source_table
        ),
        invalid_tx AS (
            SELECT DISTINCT transaction_id
            FROM bad
            WHERE transaction_id IS NOT NULL
        ),
        upd_transactions AS (
            UPDATE staging.staging_transactions t
            SET validation_status = CASE
                    WHEN EXISTS (SELECT 1 FROM invalid_tx i WHERE i.transaction_id = t.transaction_id)
                    THEN 'INVALID' ELSE 'VALID' END,
                error_codes = (
                    SELECT c.codes FROM codes c
                    WHERE c.transaction_id = t.transaction_id
                        AND c.source_table = 'transactions'
                )
            WHERE t.batch_id = %(b)s
        ),
        upd_items AS (
            UPDATE staging.staging_transaction_items i
            SET validation_status = CASE
                    WHEN EXISTS (SELECT 1 FROM invalid_tx x WHERE x.transaction_id = i.transaction_id)
                    THEN 'INVALID' ELSE 'VALID' END,
                error_codes = (
                    SELECT c.codes FROM codes c
                    WHERE c.transaction_id = i.transaction_id
                        AND c.source_table = 'transaction_items'
                )
            WHERE i.batch_id = %(b)s
        ),
        upd_payments AS (
            UPDATE staging.staging_payments p
            SET validation_status = CASE
                    WHEN EXISTS (SELECT 1 FROM invalid_tx x WHERE x.transaction_id = p.transaction_id)
                    THEN 'INVALID' ELSE 'VALID' END,
                error_codes = (
                    SELECT c.codes FROM codes c
                    WHERE c.transaction_id = p.transaction_id
                        AND c.source_table = 'payments'
                )
            WHERE p.batch_id = %(b)s
        )
        SELECT COUNT(*) FROM invalid_tx
        """,
        {"b": batch_id},
    )
    invalid_transactions = cursor.fetchone()[0]
    print(f"Batch {batch_id} divalidasi: {invalid_transactions} transaksi invalid")
    return invalid_transactions

# selesai validasi outputnya harusnya ada 2, yang sudah valid dan invalid maka buat 2 load
def load_fact_sales(cursor, batch_id):
    # guard 1: transaksi valid harus punya tepat 1 baris pembayaran (payment_key tunggal)
    cursor.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT t.transaction_id
            FROM staging.staging_transactions t
            JOIN staging.staging_payments p
              ON p.batch_id = t.batch_id AND p.transaction_id = t.transaction_id
            WHERE t.batch_id = %(b)s AND t.validation_status = 'VALID'
            GROUP BY t.transaction_id
            HAVING COUNT(*) <> 1
        ) x
        """,
        {"b": batch_id},
    )
    if cursor.fetchone()[0] > 0:
        raise ValueError(f"Batch {batch_id} punya transaksi valid dengan pembayaran != 1 baris")

    # hitung ekspektasi baris fact (valid item milik transaksi valid)
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM staging.staging_transaction_items i
        JOIN staging.staging_transactions t
          ON t.batch_id = i.batch_id AND t.transaction_id = i.transaction_id
        WHERE i.batch_id = %(b)s
          AND i.validation_status = 'VALID'
          AND t.validation_status = 'VALID'
        """,
        {"b": batch_id},
    )
    expected = cursor.fetchone()[0]

    cursor.execute(
        """
        INSERT INTO warehouse.fact_sales (
            date_key, outlet_key, product_key, payment_key,
            transaction_id, item_id, order_type, transaction_time,
            quantity, unit_price, discount_amount, line_total, line_cost,
            gross_profit, batch_id
        )
        SELECT
            d.date_key,
            o.outlet_key,
            pr.product_key,
            pm.payment_key,
            t.transaction_id,
            i.item_id,
            t.order_type,
            t.transaction_time,
            i.quantity,
            i.unit_price,
            i.discount_amount,
            i.line_total,
            i.line_cost,
            (i.line_total - i.line_cost) AS gross_profit,
            t.batch_id
        FROM staging.staging_transaction_items i
        JOIN staging.staging_transactions t
          ON t.batch_id = i.batch_id AND t.transaction_id = i.transaction_id
        JOIN staging.staging_payments p
          ON p.batch_id = i.batch_id AND p.transaction_id = i.transaction_id
        JOIN warehouse.dim_date   d  ON d.full_date     = t.transaction_date
        JOIN warehouse.dim_outlet o  ON o.outlet_id     = t.outlet_id
        JOIN warehouse.dim_product pr ON pr.product_id  = i.product_id
        JOIN warehouse.dim_payment pm ON pm.payment_method = p.payment_method
        WHERE i.batch_id = %(b)s
          AND i.validation_status = 'VALID'
          AND t.validation_status = 'VALID'
        """,
        {"b": batch_id},
    )
    loaded = cursor.rowcount

    # guard 2: kalau jumlah tidak sama, ada join dimensi yang gagal (dim_date/dim_product/dll)
    if loaded != expected:
        raise ValueError(
            f"Batch {batch_id}: fact rows {loaded} != expected {expected} "
            f"(cek dimensi: dim_date/dim_outlet/dim_product/dim_payment)"
        )

    print(f"Batch {batch_id} fact_sales: {loaded} baris")
    return loaded


def load_quarantine(cursor, batch_id):
    cursor.execute(
        """
        INSERT INTO audit.audit_quarantine_records (
            batch_id, transaction_id, source_table, error_code, error_detail
        )
        SELECT
            %(b)s,
            q.transaction_id,
            q.source_table,
            q.error_code,
            CASE q.error_code
                WHEN 'R1' THEN 'Transaksi duplikat di dalam batch'
                WHEN 'R2' THEN 'product_id tidak ditemukan di warehouse.dim_product'
                WHEN 'R3' THEN 'quantity <= 0 atau NULL'
                WHEN 'R4' THEN 'Total pembayaran tidak sama dengan total_amount'
                WHEN 'R5' THEN 'Item tanpa header transaksi'
                WHEN 'R6' THEN 'Transaksi tanpa baris pembayaran'
                WHEN 'R7' THEN 'outlet_id tidak ditemukan di warehouse.dim_outlet'
                WHEN 'R8' THEN 'Nilai gagal dikonversi ke tipe data target'
                ELSE 'Pelanggaran aturan validasi'
            END
        FROM (
            SELECT DISTINCT transaction_id, source_table, error_code
            FROM (
                SELECT 'transactions' AS source_table,
                       transaction_id,
                       unnest(string_to_array(error_codes, ',')) AS error_code
                FROM staging.staging_transactions
                WHERE batch_id = %(b)s
                  AND validation_status = 'INVALID'
                  AND error_codes IS NOT NULL

                UNION ALL

                SELECT 'transaction_items',
                       transaction_id,
                       unnest(string_to_array(error_codes, ','))
                FROM staging.staging_transaction_items
                WHERE batch_id = %(b)s
                  AND validation_status = 'INVALID'
                  AND error_codes IS NOT NULL

                UNION ALL

                SELECT 'payments',
                       transaction_id,
                       unnest(string_to_array(error_codes, ','))
                FROM staging.staging_payments
                WHERE batch_id = %(b)s
                  AND validation_status = 'INVALID'
                  AND error_codes IS NOT NULL
            ) raw
        ) q
        """,
        {"b": batch_id},
    )
    quarantined = cursor.rowcount
    print(f"Batch {batch_id} karantina: {quarantined} record")
    return quarantined

# update batch run dan disini kasi status yang diperlu 
def finish_batch_run(cursor, batch_id):
    cursor.execute(
        """
        UPDATE audit.audit_batch_runs b
        SET rows_loaded = (
                SELECT COUNT(*) FROM warehouse.fact_sales f
                WHERE f.batch_id = b.batch_id
            ),
            rows_quarantined = (
                SELECT COUNT(*) FROM audit.audit_quarantine_records q
                WHERE q.batch_id = b.batch_id
            ),
            status = CASE
                WHEN (SELECT COUNT(*) FROM audit.audit_quarantine_records q
                      WHERE q.batch_id = b.batch_id) = 0
                THEN 'SUCCESS'
                ELSE 'PARTIAL'
            END,
            finished_at = NOW()
        WHERE b.batch_id = %(b)s
        RETURNING status
        """,
        {"b": batch_id},
    )
    row = cursor.fetchone()
    status = row[0] if row else None
    print(f"Batch {batch_id} selesai: status={status}")
    return status

# semuanya akan dipanggil melalui dag sebagai 1 transaksi 