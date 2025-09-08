CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    brand TEXT,
    unit TEXT,
    base_price_value TEXT,
    base_price_unit TEXT,
    market TEXT,
    valid_from DATE,
    valid_to DATE,
    info TEXT,
    created_at DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
    product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    price_type TEXT NOT NULL,
    amount TEXT NOT NULL,
    condition TEXT,
    PRIMARY KEY (product_id, price_type)
);

CREATE TABLE IF NOT EXISTS discounts (
    product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    discount_type TEXT NOT NULL,
    amount TEXT NOT NULL,
    condition TEXT,
    PRIMARY KEY (product_id, discount_type)
);
