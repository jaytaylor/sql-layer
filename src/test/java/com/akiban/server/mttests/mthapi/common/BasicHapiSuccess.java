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
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.akiban.util.ThreadlessRandom.rand;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicHapiSuccess extends HapiSuccess {

    private final List<SaisTable> allTables;
    private final String schema;
    private final static int MAX_READ_ID = 1500;

    public BasicHapiSuccess(String schema, SaisTable root) {
        this(schema, Collections.singleton(root));
    }

    public BasicHapiSuccess(String schema, Set<SaisTable> roots) {
        assertTrue("at least one root table required", roots.size() >= 1);
        Set<SaisTable> all = SaisTable.setIncludingChildren(roots);
        for (SaisTable table : all) {
            assertTrue(table + ": only one-table-pk tables supported", table != null && table.getPK().size() == 1);
        }
        allTables = Collections.unmodifiableList(new ArrayList<SaisTable>(all));
        this.schema = schema;
    }

    @Override
    protected void validateIndex(HapiRequestStruct requestStruct, Index index) {
        HapiGetRequest request = requestStruct.getRequest();
        if (request.getUsingTable().equals(request.getSchema(), request.getTable())) {
            assertTrue("index table: " + index, index.getTableName().equals(request.getUsingTable()));
        }
        else {
            assertTrue("index should have been on group table", index.getTable().isGroupTable());
        }
        if (requestStruct.expectedIndexKnown()) {
            assertEquals("index name", requestStruct.getExpectedIndex(), index.getIndexName().getName());
        }
    }

    @Override
    protected void validateSuccessResponse(HapiRequestStruct requestStruct, JSONObject result)
            throws JSONException
    {
        try {
            JsonUtils.validateResponse(result, requestStruct.getSelectRoot(), requestStruct.getPredicatesTable());
        } catch (HapiValidationError e) {
            e.setJsonObject(result);
            throw e;
        }
    }

    @Override
    protected HapiRequestStruct pullRequest(final int pseudoRandom) {
        String idValue = Integer.toString(Math.abs(pseudoRandom) % MAX_READ_ID);
        int randTableIndex = Math.abs(pseudoRandom % allTables.size());
        SaisTable selectRoot = allTables.get(randTableIndex);
        SaisTable predicateTable = choosePredicate(selectRoot, rand(pseudoRandom));
        assert predicateTable.getPK().size() == 1 : predicateTable.getPK(); // should be verified in ctor
        String predicateColumn = predicateTable.getPK().get(0);
        HapiGetRequest request = DefaultHapiGetRequest.forTables(schema(), selectRoot.getName(), predicateTable.getName()).where(predicateColumn).eq(idValue);

        return new HapiRequestStruct(request, selectRoot, predicateTable, null);
    }

    private static SaisTable choosePredicate(SaisTable root, int pseudoRandom) {
        // for now, construct the all-children list each time. We can cache this later.
        List<SaisTable> includingChildren = new ArrayList<SaisTable>( root.setIncludingChildren() );
        int index = Math.abs(pseudoRandom % includingChildren.size() );
        return includingChildren.get(index);
    }

    public String schema() {
        return schema;
    }

    @Override
    protected int spawnCount() {
        return 500000;
    }
}
