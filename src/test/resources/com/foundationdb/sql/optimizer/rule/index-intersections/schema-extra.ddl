CREATE INDEX name ON customers(name);

CREATE INDEX "__akiban_fk_2" ON addresses(cid);
CREATE INDEX state ON addresses(state);

CREATE INDEX "__akiban_fk_3" ON referrals(cid);


CREATE INDEX "__akiban_fk_0" ON orders(cid);
CREATE INDEX order_date ON orders(order_date);

CREATE INDEX "__akiban_fk_1" ON items(oid);
CREATE INDEX sku ON items(sku);

CREATE INDEX "__akiban_fk_4" ON shipments(oid);
CREATE INDEX ship_date ON shipments(ship_date);

CREATE INDEX cname_and_sku ON customers(customers.name, items.sku) USING LEFT JOIN;
CREATE INDEX sku_and_date ON customers(items.sku, orders.order_date) USING LEFT JOIN;
