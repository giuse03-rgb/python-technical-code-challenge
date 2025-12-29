CREATE table orders (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER,
    amount     NUMERIC(10,2),
    status     VARCHAR(50),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP
);

INSERT INTO orders (user_id, amount, status)
VALUES (1, 29.99, 'CREATED');

UPDATE orders
SET status = 'PAID', updated_at = NOW()
WHERE id = 1;

UPDATE orders
SET deleted_at = NOW(), updated_at = NOW()
WHERE id = 1;

SELECT * from orders;