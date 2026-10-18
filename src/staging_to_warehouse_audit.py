# sesuai dengan aturan validasi di awal pas membuat pos generate, ada beberapa hal yang perlu divalidasi
# DUPLICATE_TRANSACTION
# INVALID_PRODUCT_ID
# INVALID_QUANTITY
# PAYMENT_MISMATCH
# sisanya akan dipikirkan pas code

# kita akan find batch dulu kaya tadi tapi dari staging
def find_staging_batches(connection):


# setelah batch ketemu kita update batch run
def register_batch_run(cursor, batch_id):

# validasi dilakukan disini sesuai dengan aturan
def validate_batch(cursor, batch_id):

# selesai validasi outputnya harusnya ada 2, yang sudah valid dan invalid maka buat 2 load
def load_fact_sales(cursor, batch_id):


def load_quarantine(cursor, batch_id):

# update batch run dan disini kasi status yang diperlu 
def finish_batch_run(cursor, batch_id):

# semuanya akan dipanggil melalui dag sebagai 1 transaksi 