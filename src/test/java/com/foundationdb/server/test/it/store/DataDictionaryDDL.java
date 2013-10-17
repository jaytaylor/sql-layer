/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.it.store;

import com.foundationdb.ais.CAOIBuilderFiller;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.session.Session;

import static com.foundationdb.ais.CAOIBuilderFiller.*;

public class DataDictionaryDDL {
    public final static String SCHEMA = "data_dictionary_test";

    public static void createTables(Session session, DDLFunctions ddl) throws InvalidOperationException {
        createTables(session, ddl, SCHEMA);
    }

    public static void createTables(Session session, DDLFunctions ddl, String schema) throws InvalidOperationException {
        NewAISBuilder builder = CAOIBuilderFiller.createAndFillBuilder(schema);
        AkibanInformationSchema tempAIS = builder.ais();

        ddl.createTable(session, tempAIS.getTable(schema, CUSTOMER_TABLE));
        ddl.createTable(session, tempAIS.getTable(schema, ADDRESS_TABLE));
        ddl.createTable(session, tempAIS.getTable(schema, ORDER_TABLE));
        ddl.createTable(session, tempAIS.getTable(schema, ITEM_TABLE));
        ddl.createTable(session, tempAIS.getTable(schema, COMPONENT_TABLE));
    }
}
