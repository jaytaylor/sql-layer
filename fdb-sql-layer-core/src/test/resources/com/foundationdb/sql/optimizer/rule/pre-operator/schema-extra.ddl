CREATE INDEX cname_and_sku ON customers(customers.name, items.sku) USING LEFT JOIN;
CREATE INDEX sku_and_date ON customers(items.sku, orders.order_date) USING LEFT JOIN;
CREATE INDEX state_and_name ON customers(addresses.state, customers.name) USING RIGHT JOIN;
