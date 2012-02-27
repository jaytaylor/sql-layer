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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.server.service.session.Session;

public class DataDictionaryDDL {
    public final static String SCHEMA = "data_dictionary_test";
    public final static String CUSTOMER_TABLE = "customer";
    public final static String ADDRESS_TABLE = "address";
    public final static String ORDER_TABLE = "order";
    public final static String ITEM_TABLE = "item";
    public final static String COMPONENT_TABLE = "component";

    private static NewAISBuilder createAndFillBuilder(String schema) {
        NewAISBuilder builder = AISBBasedBuilder.create(schema);
        
        builder.userTable(CUSTOMER_TABLE).
                colBigInt("customer_id", false).
                colString("customer_name", 100, false).
                pk("customer_id");
        
        builder.userTable(ADDRESS_TABLE).
                colBigInt("customer_id", false).
                colLong("instance_id", false).
                colString("address_line1", 60, false).
                colString("address_line2", 60, false).
                colString("address_line3", 60, false).
                pk("customer_id", "instance_id").
                joinTo("customer").on("customer_id", "customer_id");

        builder.userTable(ORDER_TABLE).
                colBigInt("order_id", false).
                colBigInt("customer_id", false).
                colLong("order_date", false).
                pk("order_id").
                joinTo("customer").on("customer_id", "customer_id");
        
        builder.userTable(ITEM_TABLE).
                colBigInt("order_id", false).
                colBigInt("part_id", false).
                colLong("quantity", false).
                colLong("unit_price", false).
                pk("part_id").
                joinTo("order").on("order_id", "order_id");

        builder.userTable(COMPONENT_TABLE).
                colBigInt("part_id", false).
                colBigInt("component_id", false).
                colLong("supplier_id", false).
                colLong("unique_id", false).
                colString("description", 50, true).
                pk("component_id").
                uniqueKey("uk", "unique_id").
                key("xk", "supplier_id").
                joinTo("item").on("part_id", "part_id");
                
        return builder;
    }

    public static void createTables(Session session, DDLFunctions ddl) throws InvalidOperationException {
        createTables(session, ddl, SCHEMA);
    }

    public static void createTables(Session session, DDLFunctions ddl, String schema) throws InvalidOperationException {
        NewAISBuilder builder = createAndFillBuilder(schema);
        AkibanInformationSchema tempAIS = builder.ais();

        ddl.createTable(session, tempAIS.getUserTable(schema, CUSTOMER_TABLE));
        ddl.createTable(session, tempAIS.getUserTable(schema, ADDRESS_TABLE));
        ddl.createTable(session, tempAIS.getUserTable(schema, ORDER_TABLE));
        ddl.createTable(session, tempAIS.getUserTable(schema, ITEM_TABLE));
        ddl.createTable(session, tempAIS.getUserTable(schema, COMPONENT_TABLE));
    }
}
