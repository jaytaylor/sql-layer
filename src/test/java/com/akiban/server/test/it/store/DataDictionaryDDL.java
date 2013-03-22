
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
