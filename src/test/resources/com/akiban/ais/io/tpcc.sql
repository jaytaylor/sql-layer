use tpcc;

CREATE TABLE `customer` (
  `c_id` int(11) NOT NULL,
  `c_d_id` tinyint(4) NOT NULL,
  `c_w_id` smallint(6) NOT NULL,
  `c_first` varchar(16) DEFAULT NULL,
  `c_middle` char(2) DEFAULT NULL,
  `c_last` varchar(16) DEFAULT NULL,
  `c_street_1` varchar(20) DEFAULT NULL,
  `c_street_2` varchar(20) DEFAULT NULL,
  `c_city` varchar(20) DEFAULT NULL,
  `c_state` char(2) DEFAULT NULL,
  `c_zip` char(9) DEFAULT NULL,
  `c_phone` char(16) DEFAULT NULL,
  `c_since` datetime DEFAULT NULL,
  `c_credit` char(2) DEFAULT NULL,
  `c_credit_lim` bigint(20) DEFAULT NULL,
  `c_discount` decimal(4,2) DEFAULT NULL,
  `c_balance` decimal(12,2) DEFAULT NULL,
  `c_ytd_payment` decimal(12,2) DEFAULT NULL,
  `c_payment_cnt` smallint(6) DEFAULT NULL,
  `c_delivery_cnt` smallint(6) DEFAULT NULL,
  `c_data` text,
  PRIMARY KEY (`c_w_id`,`c_d_id`,`c_id`),
  KEY `idx_customer` (`c_w_id`,`c_d_id`,`c_last`,`c_first`),
  CONSTRAINT `fkey_customer_1` FOREIGN KEY (`c_w_id`, `c_d_id`) REFERENCES `district` (`d_w_id`, `d_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `district` (
  `d_id` tinyint(4) NOT NULL,
  `d_w_id` smallint(6) NOT NULL,
  `d_name` varchar(10) DEFAULT NULL,
  `d_street_1` varchar(20) DEFAULT NULL,
  `d_street_2` varchar(20) DEFAULT NULL,
  `d_city` varchar(20) DEFAULT NULL,
  `d_state` char(2) DEFAULT NULL,
  `d_zip` char(9) DEFAULT NULL,
  `d_tax` decimal(4,2) DEFAULT NULL,
  `d_ytd` decimal(12,2) DEFAULT NULL,
  `d_next_o_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`d_w_id`,`d_id`),
  CONSTRAINT `fkey_district_1` FOREIGN KEY (`d_w_id`) REFERENCES `warehouse` (`w_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `history` (
  `h_c_id` int(11) DEFAULT NULL,
  `h_c_d_id` tinyint(4) DEFAULT NULL,
  `h_c_w_id` smallint(6) DEFAULT NULL,
  `h_d_id` tinyint(4) DEFAULT NULL,
  `h_w_id` smallint(6) DEFAULT NULL,
  `h_date` datetime DEFAULT NULL,
  `h_amount` decimal(6,2) DEFAULT NULL,
  `h_data` varchar(24) DEFAULT NULL,
  KEY `fkey_history_1` (`h_c_w_id`,`h_c_d_id`,`h_c_id`),
  KEY `fkey_history_2` (`h_w_id`,`h_d_id`),
  CONSTRAINT `fkey_history_1` FOREIGN KEY (`h_c_w_id`, `h_c_d_id`, `h_c_id`) REFERENCES `customer` (`c_w_id`, `c_d_id`, `c_id`),
  CONSTRAINT `fkey_history_2` FOREIGN KEY (`h_w_id`, `h_d_id`) REFERENCES `district` (`d_w_id`, `d_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `item` (
  `i_id` int(11) NOT NULL,
  `i_im_id` int(11) DEFAULT NULL,
  `i_name` varchar(24) DEFAULT NULL,
  `i_price` decimal(5,2) DEFAULT NULL,
  `i_data` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`i_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `new_orders` (
  `no_o_id` int(11) NOT NULL,
  `no_d_id` tinyint(4) NOT NULL,
  `no_w_id` smallint(6) NOT NULL,
  PRIMARY KEY (`no_w_id`,`no_d_id`,`no_o_id`),
  CONSTRAINT `fkey_new_orders_1` FOREIGN KEY (`no_w_id`, `no_d_id`, `no_o_id`) REFERENCES `orders` (`o_w_id`, `o_d_id`, `o_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `order_line` (
  `ol_o_id` int(11) NOT NULL,
  `ol_d_id` tinyint(4) NOT NULL,
  `ol_w_id` smallint(6) NOT NULL,
  `ol_number` tinyint(4) NOT NULL,
  `ol_i_id` int(11) DEFAULT NULL,
  `ol_supply_w_id` smallint(6) DEFAULT NULL,
  `ol_delivery_d` datetime DEFAULT NULL,
  `ol_quantity` tinyint(4) DEFAULT NULL,
  `ol_amount` decimal(6,2) DEFAULT NULL,
  `ol_dist_info` char(24) DEFAULT NULL,
  PRIMARY KEY (`ol_w_id`,`ol_d_id`,`ol_o_id`,`ol_number`),
  KEY `fkey_order_line_2` (`ol_supply_w_id`,`ol_i_id`),
  CONSTRAINT `fkey_order_line_1` FOREIGN KEY (`ol_w_id`, `ol_d_id`, `ol_o_id`) REFERENCES `orders` (`o_w_id`, `o_d_id`, `o_id`),
  CONSTRAINT `fkey_order_line_2` FOREIGN KEY (`ol_supply_w_id`, `ol_i_id`) REFERENCES `stock` (`s_w_id`, `s_i_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `orders` (
  `o_id` int(11) NOT NULL,
  `o_d_id` tinyint(4) NOT NULL,
  `o_w_id` smallint(6) NOT NULL,
  `o_c_id` int(11) DEFAULT NULL,
  `o_entry_d` datetime DEFAULT NULL,
  `o_carrier_id` tinyint(4) DEFAULT NULL,
  `o_ol_cnt` tinyint(4) DEFAULT NULL,
  `o_all_local` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`o_w_id`,`o_d_id`,`o_id`),
  KEY `idx_orders` (`o_w_id`,`o_d_id`,`o_c_id`,`o_id`),
  CONSTRAINT `fkey_orders_1` FOREIGN KEY (`o_w_id`, `o_d_id`, `o_c_id`) REFERENCES `customer` (`c_w_id`, `c_d_id`, `c_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `stock` (
  `s_i_id` int(11) NOT NULL,
  `s_w_id` smallint(6) NOT NULL,
  `s_quantity` smallint(6) DEFAULT NULL,
  `s_dist_01` char(24) DEFAULT NULL,
  `s_dist_02` char(24) DEFAULT NULL,
  `s_dist_03` char(24) DEFAULT NULL,
  `s_dist_04` char(24) DEFAULT NULL,
  `s_dist_05` char(24) DEFAULT NULL,
  `s_dist_06` char(24) DEFAULT NULL,
  `s_dist_07` char(24) DEFAULT NULL,
  `s_dist_08` char(24) DEFAULT NULL,
  `s_dist_09` char(24) DEFAULT NULL,
  `s_dist_10` char(24) DEFAULT NULL,
  `s_ytd` decimal(8,0) DEFAULT NULL,
  `s_order_cnt` smallint(6) DEFAULT NULL,
  `s_remote_cnt` smallint(6) DEFAULT NULL,
  `s_data` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`s_w_id`,`s_i_id`),
  KEY `fkey_stock_2` (`s_i_id`),
  CONSTRAINT `fkey_stock_1` FOREIGN KEY (`s_w_id`) REFERENCES `warehouse` (`w_id`),
  CONSTRAINT `fkey_stock_2` FOREIGN KEY (`s_i_id`) REFERENCES `item` (`i_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `warehouse` (
  `w_id` smallint(6) NOT NULL,
  `w_name` varchar(10) DEFAULT NULL,
  `w_street_1` varchar(20) DEFAULT NULL,
  `w_street_2` varchar(20) DEFAULT NULL,
  `w_city` varchar(20) DEFAULT NULL,
  `w_state` char(2) DEFAULT NULL,
  `w_zip` char(9) DEFAULT NULL,
  `w_tax` decimal(4,2) DEFAULT NULL,
  `w_ytd` decimal(12,2) DEFAULT NULL,
  PRIMARY KEY (`w_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
