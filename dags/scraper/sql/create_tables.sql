CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    brand TEXT,
    unit TEXT,
    base_price_value TEXT,
    base_price_unit TEXT,
    market TEXT,
    valid_from TEXT,
    valid_to TEXT,
    info TEXT
);

CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    price_type TEXT NOT NULL,
    amount TEXT NOT NULL,
    condition TEXT
);

CREATE TABLE IF NOT EXISTS discounts (
    id SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    discount_type TEXT NOT NULL,
    amount TEXT NOT NULL,
    condition TEXT
);
