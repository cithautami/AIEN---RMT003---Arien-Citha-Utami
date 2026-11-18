BEGIN;
SET LOCAL TIME ZONE 'UTC';
CREATE SCHEMA IF NOT EXISTS silver;


CREATE TABLE IF NOT EXISTS silver.city_reference (
    city_id TEXT PRIMARY KEY,
    city_name TEXT NOT NULL,
    country TEXT NOT NULL,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.sales_channels (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id TEXT PRIMARY KEY,
    city_id TEXT NOT NULL REFERENCES silver.city_reference(city_id),
    customer_segment TEXT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.customer_profiles (
    customer_id TEXT NOT NULL REFERENCES silver.customers(customer_id),
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ,
    city_id TEXT NOT NULL REFERENCES silver.city_reference(city_id),
    customer_segment TEXT NOT NULL,
    PRIMARY KEY (customer_id, valid_from_utc),
    CHECK (valid_to_utc IS NULL OR valid_to_utc > valid_from_utc),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.customer_addresses (
    address_id TEXT NOT NULL,
    customer_id TEXT NOT NULL REFERENCES silver.customers(customer_id),
    city_id TEXT NOT NULL REFERENCES silver.city_reference(city_id),
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ,
    PRIMARY KEY (address_id, valid_from_utc),
    CHECK (valid_to_utc IS NULL OR valid_to_utc > valid_from_utc),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    sku TEXT NOT NULL,
    unit_price NUMERIC(14, 2) NOT NULL CHECK (unit_price >= 0),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.product_categories (
    product_id TEXT NOT NULL REFERENCES silver.products(product_id),
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ,
    category_id TEXT NOT NULL,
    category_name TEXT NOT NULL,
    PRIMARY KEY (product_id, valid_from_utc),
    CHECK (valid_to_utc IS NULL OR valid_to_utc > valid_from_utc),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.stores (
    store_id TEXT PRIMARY KEY,
    store_name TEXT NOT NULL,
    city_id TEXT NOT NULL REFERENCES silver.city_reference(city_id),
    location_type TEXT NOT NULL,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.promotions (
    promotion_id TEXT PRIMARY KEY,
    promotion_code TEXT NOT NULL,
    promotion_type TEXT NOT NULL,
    discount_rate NUMERIC(14, 6) NOT NULL CHECK (discount_rate >= 0),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    CHECK (end_date >= start_date),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES silver.customers(customer_id),
    ordered_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ,
    sales_channel TEXT NOT NULL REFERENCES silver.sales_channels(channel_id),
    status TEXT NOT NULL,
    store_id TEXT REFERENCES silver.stores(store_id),
    shipping_revenue NUMERIC(14, 2) NOT NULL CHECK (shipping_revenue >= 0),
    profile_valid_from_utc TIMESTAMPTZ,
    FOREIGN KEY (customer_id, profile_valid_from_utc)
        REFERENCES silver.customer_profiles(customer_id, valid_from_utc),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

-- Compatibility with the existing silver.orders from earlier work.
-- Historical rows have no known Silver run ID; do not invent one.
-- The Silver rebuild will populate pipeline_run_id and temporal selection.
ALTER TABLE silver.orders
    ADD COLUMN IF NOT EXISTS pipeline_run_id TEXT REFERENCES ops.pipeline_runs(pipeline_run_id),
    ADD COLUMN IF NOT EXISTS profile_valid_from_utc TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
    product_id TEXT NOT NULL REFERENCES silver.products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(14, 2) NOT NULL CHECK (unit_price >= 0),
    item_discount_amount NUMERIC(14, 2) NOT NULL CHECK (item_discount_amount >= 0),
    category_valid_from_utc TIMESTAMPTZ,
    FOREIGN KEY (product_id, category_valid_from_utc)
        REFERENCES silver.product_categories(product_id, valid_from_utc),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.order_promotions (
    order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
    promotion_id TEXT NOT NULL REFERENCES silver.promotions(promotion_id),
    discount_amount NUMERIC(14, 2) NOT NULL CHECK (discount_amount >= 0),
    PRIMARY KEY (order_id, promotion_id),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.payment_events (
    event_id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL,
    order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('PAYMENT_AUTHORIZED', 'PAYMENT_CAPTURED', 'PAYMENT_FAILED')),
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    amount NUMERIC(14, 2) NOT NULL CHECK (amount >= 0),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.refund_events (
    event_id TEXT PRIMARY KEY,
    refund_id TEXT NOT NULL,
    order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('REFUND_ISSUED', 'REFUND_COMPLETED')),
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    amount NUMERIC(14, 2) NOT NULL CHECK (amount >= 0),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.return_events (
    event_id TEXT PRIMARY KEY,
    return_id TEXT NOT NULL,
    order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('RETURN_REQUESTED', 'RETURN_RECEIVED', 'RETURN_CLOSED')),
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.support_events (
    event_id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    order_id TEXT NOT NULL REFERENCES silver.orders(order_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('SUPPORT_TICKET_CREATED', 'SUPPORT_TICKET_CLOSED')),
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    customer_id TEXT NOT NULL REFERENCES silver.customers(customer_id),
    reason TEXT,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.web_events (
    event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    customer_id TEXT REFERENCES silver.customers(customer_id),
    order_id TEXT REFERENCES silver.orders(order_id),
    campaign_id TEXT,
    sales_channel TEXT NOT NULL REFERENCES silver.sales_channels(channel_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('PAGE_VIEW', 'ADD_TO_CART', 'PURCHASE')),
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    product_id TEXT NOT NULL REFERENCES silver.products(product_id),
    location_id TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity INTEGER NOT NULL CHECK (reserved_quantity >= 0),
    unit_cost NUMERIC(14, 2) NOT NULL CHECK (unit_cost >= 0),
    PRIMARY KEY (product_id, location_id, snapshot_date),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    spend_date DATE NOT NULL,
    campaign_id TEXT NOT NULL,
    sales_channel TEXT NOT NULL REFERENCES silver.sales_channels(channel_id),
    spend_amount NUMERIC(14, 2) NOT NULL CHECK (spend_amount >= 0),
    PRIMARY KEY (spend_date, sales_channel, campaign_id),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver.rejected_records (
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    source_row_number INTEGER NOT NULL CHECK (source_row_number > 0),
    reason_code TEXT NOT NULL,
    reason_detail TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline_run_id, source_file, file_sha256, source_row_number, reason_code),
    FOREIGN KEY (source_file, file_sha256, source_row_number)
        REFERENCES bronze.raw_records(source_file, file_sha256, source_row_number)
);

-- Locations can include warehouses that are not physical retail stores.
-- Thus location_id is not restricted to silver.stores.
-- Campaign IDs have no supplied master table; no fictional master is created.
-- Missing inventory is represented by absence, not by an invented zero.

CREATE INDEX IF NOT EXISTS silver_orders_ordered_at_idx
    ON silver.orders (ordered_at_utc);
CREATE INDEX IF NOT EXISTS silver_order_items_order_idx
    ON silver.order_items (order_id);
CREATE INDEX IF NOT EXISTS silver_order_items_product_idx
    ON silver.order_items (product_id);
CREATE INDEX IF NOT EXISTS silver_payment_identity_idx
    ON silver.payment_events (payment_id, occurred_at_utc, ingested_at_utc);
CREATE INDEX IF NOT EXISTS silver_refund_identity_idx
    ON silver.refund_events (refund_id, occurred_at_utc, ingested_at_utc);
CREATE INDEX IF NOT EXISTS silver_return_identity_idx
    ON silver.return_events (return_id, occurred_at_utc, ingested_at_utc);
CREATE INDEX IF NOT EXISTS silver_support_identity_idx
    ON silver.support_events (ticket_id, occurred_at_utc, ingested_at_utc);
CREATE INDEX IF NOT EXISTS silver_web_order_time_idx
    ON silver.web_events (order_id, occurred_at_utc);
CREATE INDEX IF NOT EXISTS silver_inventory_date_idx
    ON silver.inventory_snapshots (snapshot_date, product_id);
COMMIT;
