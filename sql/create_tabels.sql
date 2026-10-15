-- Schema: raw
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.raw_transactions(
    batch_id VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    transaction_id TEXT,
    outlet_id TEXT,
    transaction_time TEXT,
    order_type TEXT,
    payment_method TEXT,
    status TEXT,
    total_amount TEXT
);

CREATE TABLE IF NOT EXISTS raw.raw_transaction_items(
    batch_id VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    transaction_id TEXT,
    item_id TEXT,
    product_id TEXT,
    quantity TEXT,
    unit_price TEXT,
    discount_amount TEXT,
    line_total TEXT,
    line_cost TEXT
);

CREATE TABLE IF NOT EXISTS raw.raw_payments(
    batch_id VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    payment_id TEXT,
    transaction_id TEXT,
    payment_method TEXT,
    paid_amount TEXT,
    payment_status TEXT
);

--Schema: staging
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.staging_transactions(
    batch_id VARCHAR(50) NOT NULL,
    validation_status VARCHAR(10) DEFAULT('PENDING'),
    error_codes VARCHAR(100),
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    transaction_id VARCHAR(20),
    outlet_id VARCHAR(10),
    transaction_time TIMESTAMP,
    transaction_date DATE,
    order_type VARCHAR(20),
    payment_method VARCHAR(20),
    status VARCHAR(20),
    total_amount NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS staging.staging_transaction_items(
    batch_id VARCHAR(50) NOT NULL,
    validation_status VARCHAR(10) DEFAULT('PENDING'),
    error_codes VARCHAR(100),
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    transaction_id VARCHAR(20),
    item_id VARCHAR(20),
    product_id VARCHAR(10),
    quantity INTEGER,
    unit_price NUMERIC(14,2),
    discount_amount NUMERIC(14,2),
    line_total NUMERIC(14,2),
    line_cost NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS staging.staging_payments(
    batch_id VARCHAR(50) NOT NULL,
    validation_status VARCHAR(10) DEFAULT('PENDING'),
    error_codes VARCHAR(100),
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    payment_id VARCHAR(20),
    transaction_id VARCHAR(20),
    payment_method VARCHAR(20),
    paid_amount NUMERIC(14,2),
    payment_status VARCHAR(20)
);

--Schema Audit
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.audit_batch_runs(
    batch_id VARCHAR(50) PRIMARY KEY,
    outlet_id VARCHAR(10) NOT NULL,
    batch_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    rows_loaded INTEGER,
    rows_quarantined INTEGER,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS audit.audit_quarantine_records(
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL REFERENCES audit.audit_batch_runs(batch_id),
    transaction_id VARCHAR(20) NOT NULL,
    source_table VARCHAR(30),
    error_code VARCHAR(10) NOT NULL,
    error_detail TEXT,
    quarantined_at TIMESTAMPTZ DEFAULT NOW()
);

--Schema: Warehouse
CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_outlet(
    outlet_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    outlet_id VARCHAR(10) UNIQUE NOT NULL,
    outlet_name VARCHAR(100),
    city VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product(
    product_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id VARCHAR(10) UNIQUE NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(30),
    list_unit_price NUMERIC(14,2),
    list_unit_cost NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_payment(
    payment_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_method VARCHAR(20) NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    UNIQUE (payment_method, payment_status)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date(
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week SMALLINT,
    day_name VARCHAR(10),
    day_of_month SMALLINT,
    week_of_year SMALLINT,
    month_number SMALLINT,
    month_name VARCHAR(10),
    quarter SMALLINT,
    year SMALLINT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.fact_sales(
    sales_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    outlet_key INTEGER NOT NULL REFERENCES warehouse.dim_outlet(outlet_key),
    product_key INTEGER NOT NULL REFERENCES warehouse.dim_product(product_key),
    payment_key INTEGER NOT NULL REFERENCES warehouse.dim_payment(payment_key),
    transaction_id VARCHAR(20) NOT NULL,
    item_id VARCHAR(20) NOT NULL UNIQUE,
    order_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(14,2) NOT NULL,
    discount_amount NUMERIC(14,2) NOT NULL,
    line_total NUMERIC(14,2) NOT NULL,
    line_cost NUMERIC(14,2) NOT NULL,
    gross_profit NUMERIC(14,2) NOT NULL,
    batch_id VARCHAR(50) NOT NULL REFERENCES audit.audit_batch_runs(batch_id),
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);

--INDEXES for raw
CREATE INDEX IF NOT EXISTS idx_raw_transactions_batch_id
    ON raw.raw_transactions(batch_id);

CREATE INDEX IF NOT EXISTS idx_raw_transaction_items_batch_id
    ON raw.raw_transaction_items(batch_id);

CREATE INDEX IF NOT EXISTS idx_raw_payments_batch_id
    ON raw.raw_payments(batch_id);
