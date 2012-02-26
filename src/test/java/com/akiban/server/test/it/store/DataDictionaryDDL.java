/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.store;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.server.service.session.Session;

public class DataDictionaryDDL {
    public final static String SCHEMA = "data_dictionary_test";

    public final static String CUSTOMER_DDL =
        "create table customer("+
            "customer_id bigint not null,"+
            "customer_name varchar(100) not null,"+
            "primary key(customer_id)"+
        ") engine = akibandb;";

    public final static String ADDRESS_DDL =
        "create table `address`("+
            "customer_id bigint not null,"+
            "instance_id int not null,"+
            "address_line1 varchar(60) not null,"+
            "address_line2 varchar(60) not null,"+
            "address_line3 varchar(60) not null,"+
            "primary key (`customer_id`, `instance_id`),"+
            "constraint __akiban foreign key(customer_id) references customer(customer_id)"+
        ") engine = akibandb;";

    public final static String ORDER_DDL =
        "create table `order`("+
            "order_id bigint not null,"+
            "customer_id bigint not null,"+
            "order_date int not null,"+
            "primary key(order_id),"+
            "foreign key(customer_id) references customer(customer_id),"+
            "constraint __akiban foreign key(customer_id) references customer(customer_id)"+
        ") engine = akibandb;";

    public final static String ITEM_DDL =
        "create table item("+
            "order_id bigint not null,"+
            "part_id bigint not null,"+
            "quantity int not null,"+
            "unit_price int not null,"+
            "primary key(part_id),"+
            "foreign key(order_id) references `order`(order_id),"+
            "constraint __akiban FOREIGN KEY(order_id) references `order`(order_id)"+
        ") engine = akibandb;";

    public final static String COMPONENT_DDL =
        "create table component("+
            "part_id bigint not null,"+
            "component_id bigint not null,"+
            "supplier_id int not null,"+
            "unique_id int not null,"+
            "description varchar(50),"+
            "primary key (`component_id`),"+
            "foreign key `fk` (`part_id`) references item(`part_id`),"+
            "unique key `uk` (`unique_id`),"+
            "key `xk` (supplier_id),"+
            "constraint __akiban foreign key(part_id) references item(part_id)"+
        ") engine = akibandb;";


    public static void createTables(Session session, DDLFunctions ddl) throws InvalidOperationException {
        createTables(session, ddl, SCHEMA);
    }

    public static void createTables(Session session, DDLFunctions ddl, String schema) throws InvalidOperationException {
        throw new UnsupportedOperationException("Reimplement");
        /*
        ddl.createTable(session, schema, CUSTOMER_DDL);
        ddl.createTable(session, schema, ADDRESS_DDL);
        ddl.createTable(session, schema, ORDER_DDL);
        ddl.createTable(session, schema, ITEM_DDL);
        ddl.createTable(session, schema, COMPONENT_DDL);
        */
    }
}
