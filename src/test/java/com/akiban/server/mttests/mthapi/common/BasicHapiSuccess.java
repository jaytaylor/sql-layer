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

package com.akiban.server.mttests.mthapi.common;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.mttests.mthapi.base.HapiSuccess;
import com.akiban.server.service.memcache.SimpleHapiPredicate;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BasicHapiSuccess extends HapiSuccess {

    private final int MAX_READ_ID;

    public BasicHapiSuccess(int MAX_READ_ID) {
        this.MAX_READ_ID = MAX_READ_ID;
    }

    @Override
    protected void validateIndex(HapiGetRequest request, Index index) {
        assertTrue("index table: " + index, index.getTableName().equals("s1", "c"));
        assertEquals("index name", "PRIMARY", index.getIndexName().getName());
    }

    @Override
    protected void validateSuccessResponse(HapiGetRequest request, JSONObject result)
            throws JSONException
    {
        JSONArray customers = result.getJSONArray("@c");

        int customersCount = customers.length();
        assertFalse(String.format("too many customsers (%d): %s -> %s", customersCount, request, result),
                customersCount > 1);

        if (customers.length() > 0) {
            JSONObject customer = customers.getJSONObject(0);
            Set<String> cKeys = JsonUtils.jsonObjectKeys(customer);
            assertEquals(cKeys + " length", 3, cKeys.size());
            final int cID = JsonUtils.jsonObjectInt(customer, "id", request);
            assertEquals("customer id", request.getPredicates().get(0).getValue(), Integer.toString(cID));
            assertTrue("customer missing age: " + result, cKeys.contains("age"));
            JSONArray orders = customer.getJSONArray("@o");

            for (int ordersLen=orders.length(), oIndex=0; oIndex < ordersLen; ++oIndex ) {
                JSONObject order = orders.getJSONObject(oIndex);
                Set<String> oKeys = JsonUtils.jsonObjectKeys(order);
                assertEquals(oKeys + " length", 3, oKeys.size());
                final int oID = JsonUtils.jsonObjectInt(order, "id", request);
                Assert.assertEquals("cid", cID, JsonUtils.jsonObjectInt(order, "cid", request));
                JSONArray items = order.getJSONArray("@i");


                for (int itemsLen=items.length(), iIndex=0; iIndex < itemsLen; ++iIndex) {
                    JSONObject item = items.getJSONObject(iIndex);
                    Set<String> iKeys = JsonUtils.jsonObjectKeys(item);
                    assertEquals(iKeys + " length", 2, iKeys.size());
                    assertTrue("item lacking id: " + result, iKeys.contains("id"));
                    Assert.assertEquals("item's order", oID, JsonUtils.jsonObjectInt(item, "oid", request));
                }
            }
        }
    }

    @Override
    protected HapiGetRequest pullRequest(final int pseudoRandom) {
        return new HapiGetRequest() {
            private final String idValue = Integer.toString(Math.abs(pseudoRandom) % MAX_READ_ID);
            private final TableName using = new TableName("s1", "c");
            @Override
            public String getSchema() {
                return using.getSchemaName();
            }

            @Override
            public String getTable() {
                return using.getTableName();
            }

            @Override
            public TableName getUsingTable() {
                return using;
            }

            @Override
            public List<HapiPredicate> getPredicates() {
                return Arrays.<HapiPredicate>asList(
                        new SimpleHapiPredicate(using, "id", HapiPredicate.Operator.EQ, idValue)
                );
            }

            @Override
            public String toString() {
                return String.format("%s:%s:%s=%s", getSchema(), getTable(), "id", idValue);
            }
        };
    }

    @Override
    protected int spawnCount() {
        return 10000;
    }
}
