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

package com.akiban.server.test.it.hapi;

import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.memcache.ParsedHapiGetRequest;
import com.akiban.server.service.memcache.hprocessor.Scanrows;
import com.akiban.server.service.memcache.outputter.jsonoutputter.JsonOutputter;
import com.akiban.server.test.it.ITBase;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static junit.framework.Assert.assertTrue;

public class MessageSizeLimitIT extends ITBase
{
    @Test
    public void test() throws Exception
    {
        createSchema();
        populate();
        runQueries();
    }

    protected Collection<Property> startupConfigProperties()
    {
        return Collections.singleton(new Property("akserver.hapi.scanrows.messageSizeBytes", "200"));
    }

    private void createSchema() throws InvalidOperationException
    {
        testTable = createTable(SCHEMA, TABLE,
                                "id int not null",
                                "filler varchar(200)",
                                "primary key(id)");
    }

    private void populate() throws Exception
    {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < FILLER_SIZE; i++) {
            buffer.append('x');
        }
        String filler = buffer.toString();
        for (int id = 0; id < ROWS; id++) {
            NewRow row = createNewRow(testTable, id, filler);
            dml().writeRow(session(), row);
        }
    }

    private void runQueries() throws Exception
    {
        int previousQueryResultLength = -1;
        for (int resultRows = 0; resultRows <= ROWS; resultRows++) {
            String queryResult = runQuery(ROWS - resultRows);
            assertTrue(queryResult.length() <= MESSAGE_LIMIT_BYTES);
            assertTrue(queryResult.length() >= previousQueryResultLength);
            previousQueryResultLength = queryResult.length();
        }
    }

    private String runQuery(int minId) throws Exception
    {
        String query = query(minId);
        HapiGetRequest request = ParsedHapiGetRequest.parse(query);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(MESSAGE_LIMIT_BYTES);
        Scanrows.instance(configService(), dxl()).processRequest(session(), request, outputter, outputStream);
        return new String(outputStream.toByteArray());
    }

    private String query(int minId)
    {
        return String.format("%s:%s:id>=%s", SCHEMA, TABLE, minId);
    }

    private String formatJSON(String json) throws JSONException
    {
        return new JSONObject(json).toString(4);
    }

    void print(String template, Object... args)
    {
        System.out.println(String.format(template, args));
    }

    // Test parameters
    private static final int MESSAGE_LIMIT_BYTES = 200;
    private static final int ROWS = 10;
    private static final int FILLER_SIZE = 100;

    // Constants
    private static final String SCHEMA = "schema";
    private static final String TABLE = "t";

    // Test state
    private int testTable;
    private final JsonOutputter outputter = new JsonOutputter();

}
