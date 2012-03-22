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

import com.akiban.ais.CAOIBuilderFiller;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.session.Session;

import static com.akiban.ais.CAOIBuilderFiller.*;

public class DataDictionaryDDL {
    public final static String SCHEMA = "data_dictionary_test";

    public static void createTables(Session session, DDLFunctions ddl) throws InvalidOperationException {
        createTables(session, ddl, SCHEMA);
    }

    public static void createTables(Session session, DDLFunctions ddl, String schema) throws InvalidOperationException {
        NewAISBuilder builder = CAOIBuilderFiller.createAndFillBuilder(schema);
        AkibanInformationSchema tempAIS = builder.ais();

        ddl.createTable(session, tempAIS.getUserTable(schema, CUSTOMER_TABLE));
        ddl.createTable(session, tempAIS.getUserTable(schema, ADDRESS_TABLE));
        ddl.createTable(session, tempAIS.getUserTable(schema, ORDER_TABLE));
        ddl.createTable(session, tempAIS.getUserTable(schema, ITEM_TABLE));
        ddl.createTable(session, tempAIS.getUserTable(schema, COMPONENT_TABLE));
    }
}
