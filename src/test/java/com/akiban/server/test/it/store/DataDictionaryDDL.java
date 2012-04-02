/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
