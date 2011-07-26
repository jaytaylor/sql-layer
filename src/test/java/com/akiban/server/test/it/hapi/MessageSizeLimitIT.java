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

package com.akiban.server.test.it.hapi;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.dml.scan.NewRow;
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
        return Collections.singleton(new Property("akserver.maxHAPIMessageSizeBytes", "200"));
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
        Scanrows.instance().processRequest(session(), request, outputter, outputStream);
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
