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
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.hapi.DefaultHapiGetRequest;
import com.akiban.server.mttests.mthapi.base.HapiRequestStruct;
import com.akiban.server.mttests.mthapi.base.HapiSuccess;
import com.akiban.server.mttests.mthapi.base.sais.SaisBuilder;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import org.json.JSONException;
import org.json.JSONObject;

import static org.junit.Assert.assertEquals;
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
    protected void validateSuccessResponse(HapiRequestStruct requestStruct, JSONObject result)
            throws JSONException
    {
        JsonUtils.validateResponse(result, requestStruct.getSelectRoot(), requestStruct.getPredicatesTable());
    }

    @Override
    protected HapiRequestStruct pullRequest(final int pseudoRandom) {
        String idValue = Integer.toString(Math.abs(pseudoRandom) % MAX_READ_ID);
        HapiGetRequest request = DefaultHapiGetRequest.forTables("s1", "c", "c").where("id").eq(idValue);

        SaisBuilder builder = new SaisBuilder();
        builder.table("c", "id", "age").pk("id");
        builder.table("o", "id", "cid").pk("id").joinTo("c").col("id", "cid");
        builder.table("i", "id", "oid").pk("id").joinTo("o").col("id", "oid");
        SaisTable table = builder.getSoleRootTable();
        return new HapiRequestStruct(request, table);
    }

    @Override
    protected int spawnCount() {
        return 10000;
    }
}
