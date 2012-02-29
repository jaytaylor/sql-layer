CREATE TABLE tpcc.customer(
  c_id int NOT NULL,
  c_d_id int NOT NULL,
  c_w_id smallint NOT NULL,
  c_first varchar(16) DEFAULT NULL,
  c_middle char(2) DEFAULT NULL,
  c_last varchar(16) DEFAULT NULL,
  c_street_1 varchar(20) DEFAULT NULL,
  c_street_2 varchar(20) DEFAULT NULL,
  c_city varchar(20) DEFAULT NULL,
  c_state char(2) DEFAULT NULL,
  c_zip char(9) DEFAULT NULL,
  c_phone char(16) DEFAULT NULL,
  c_since datetime DEFAULT NULL,
  c_credit char(2) DEFAULT NULL,
  c_credit_lim bigint DEFAULT NULL,
  c_discount decimal(4,2) DEFAULT NULL,
  c_balance decimal(12,2) DEFAULT NULL,
  c_ytd_payment decimal(12,2) DEFAULT NULL,
  c_payment_cnt smallint DEFAULT NULL,
  c_delivery_cnt smallint DEFAULT NULL,
  c_data clob,
  PRIMARY KEY (c_w_id,c_d_id,c_id)
);
CREATE INDEX idx_customer ON tpcc.customer(c_w_id,c_d_id,c_last,c_first);

CREATE TABLE tpcc.district (
  d_id int NOT NULL,
  d_w_id smallint NOT NULL,
  d_name varchar(10) DEFAULT NULL,
  d_street_1 varchar(20) DEFAULT NULL,
  d_street_2 varchar(20) DEFAULT NULL,
  d_city varchar(20) DEFAULT NULL,
  d_state char(2) DEFAULT NULL,
  d_zip char(9) DEFAULT NULL,
  d_tax decimal(4,2) DEFAULT NULL,
  d_ytd decimal(12,2) DEFAULT NULL,
  d_next_o_id int DEFAULT NULL,
  PRIMARY KEY (d_w_id,d_id)
);

CREATE TABLE tpcc.history (
  h_c_id int DEFAULT NULL,
  h_c_d_id int DEFAULT NULL,
  h_c_w_id smallint DEFAULT NULL,
  h_d_id int DEFAULT NULL,
  h_w_id smallint DEFAULT NULL,
  h_date datetime DEFAULT NULL,
  h_amount decimal(6,2) DEFAULT NULL,
  h_data varchar(24) DEFAULT NULL
);
CREATE INDEX fkey_history_1 ON tpcc.history(h_c_w_id,h_c_d_id,h_c_id);
CREATE INDEX fkey_history_2 ON tpcc.history(h_w_id,h_d_id);

CREATE TABLE tpcc.item (
  i_id int NOT NULL,
  i_im_id int DEFAULT NULL,
  i_name varchar(24) DEFAULT NULL,
  i_price decimal(5,2) DEFAULT NULL,
  i_data varchar(50) DEFAULT NULL,
  PRIMARY KEY (i_id)
);

CREATE TABLE tpcc.new_orders (
  no_o_id int NOT NULL,
  no_d_id int NOT NULL,
  no_w_id smallint NOT NULL,
  PRIMARY KEY (no_w_id,no_d_id,no_o_id)
);

CREATE TABLE tpcc.order_line (
  ol_o_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_w_id smallint NOT NULL,
  ol_number int NOT NULL,
  ol_i_id int DEFAULT NULL,
  ol_supply_w_id smallint DEFAULT NULL,
  ol_delivery_d datetime DEFAULT NULL,
  ol_quantity int DEFAULT NULL,
  ol_amount decimal(6,2) DEFAULT NULL,
  ol_dist_info char(24) DEFAULT NULL,
  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)

);
CREATE INDEX fkey_order_line_2 ON tpcc.order_line(ol_supply_w_id,ol_i_id);

CREATE TABLE tpcc.orders (
  o_id int NOT NULL,
  o_d_id int NOT NULL,
  o_w_id smallint NOT NULL,
  o_c_id int DEFAULT NULL,
  o_entry_d datetime DEFAULT NULL,
  o_carrier_id int DEFAULT NULL,
  o_ol_cnt int DEFAULT NULL,
  o_all_local int DEFAULT NULL,
  PRIMARY KEY (o_w_id,o_d_id,o_id)
);
CREATE INDEX idx_orders ON tpcc.orders(o_w_id,o_d_id,o_c_id,o_id);

CREATE TABLE tpcc.stock (
  s_i_id int NOT NULL,
  s_w_id smallint NOT NULL,
  s_quantity smallint DEFAULT NULL,
  s_dist_01 char(24) DEFAULT NULL,
  s_dist_02 char(24) DEFAULT NULL,
  s_dist_03 char(24) DEFAULT NULL,
  s_dist_04 char(24) DEFAULT NULL,
  s_dist_05 char(24) DEFAULT NULL,
  s_dist_06 char(24) DEFAULT NULL,
  s_dist_07 char(24) DEFAULT NULL,
  s_dist_08 char(24) DEFAULT NULL,
  s_dist_09 char(24) DEFAULT NULL,
  s_dist_10 char(24) DEFAULT NULL,
  s_ytd decimal(8,0) DEFAULT NULL,
  s_order_cnt smallint DEFAULT NULL,
  s_remote_cnt smallint DEFAULT NULL,
  s_data varchar(50) DEFAULT NULL,
  PRIMARY KEY (s_w_id,s_i_id)
);
CREATE INDEX fkey_stock_2 ON tpcc.stock(s_i_id);

CREATE TABLE tpcc.warehouse (
  w_id smallint NOT NULL,
  w_name varchar(10) DEFAULT NULL,
  w_street_1 varchar(20) DEFAULT NULL,
  w_street_2 varchar(20) DEFAULT NULL,
  w_city varchar(20) DEFAULT NULL,
  w_state char(2) DEFAULT NULL,
  w_zip char(9) DEFAULT NULL,
  w_tax decimal(4,2) DEFAULT NULL,
  w_ytd decimal(12,2) DEFAULT NULL,
  PRIMARY KEY (w_id)
);
