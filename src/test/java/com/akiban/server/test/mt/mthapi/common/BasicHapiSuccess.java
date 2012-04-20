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

package com.akiban.server.test.mt.mthapi.common;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.hapi.DefaultHapiGetRequest;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.test.mt.mthapi.base.HapiRequestStruct;
import com.akiban.server.test.mt.mthapi.base.HapiSuccess;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import com.akiban.util.ThreadlessRandom;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicHapiSuccess extends HapiSuccess {

    private final List<SaisTable> allTables;
    private final String schema;
    private final static int MAX_READ_ID = 1500;

    public BasicHapiSuccess(String schema, SaisTable root, boolean requireSingleColPKs) {
        this(schema, Collections.singleton(root), requireSingleColPKs);
    }

    public BasicHapiSuccess(String schema, Set<SaisTable> roots, boolean requireSingleColPKs) {
        assertTrue("at least one root table required", roots.size() >= 1);
        Set<SaisTable> all = SaisTable.setIncludingChildren(roots);
        if (requireSingleColPKs) {
            for (SaisTable table : all) {
                assertTrue(table + ": only one-table-pk tables supported", table != null && table.getPK().size() == 1);
            }
        }
        allTables = Collections.unmodifiableList(new ArrayList<SaisTable>(all));
        this.schema = schema;
    }

    @Override
    protected void validateIndex(HapiRequestStruct requestStruct, Index index) {
        HapiGetRequest request = requestStruct.getRequest();
        assertTrue("index is TableIndex: " + index, index.isTableIndex());
        TableIndex tableIndex = (TableIndex)index;
        if (request.getUsingTable().equals(request.getSchema(), request.getTable())) {
            assertTrue("index table: " + index, tableIndex.getTable().getName().equals(request.getUsingTable()));
        }
        else {
            assertTrue("index should have been on group table", tableIndex.getTable().isGroupTable());
        }
        if (requestStruct.expectedIndexKnown()) {
            assertEquals("index name", requestStruct.getExpectedIndex(), index.getIndexName().getName());
        }
    }

    @Override
    protected void validateSuccessResponse(HapiRequestStruct requestStruct, JSONObject result)
            throws JSONException
    {
        JsonUtils.validateResponse(result, requestStruct.getSelectRoot(), requestStruct.getPredicatesTable());
    }

    @Override
    protected HapiRequestStruct pullRequest(ThreadlessRandom random) {
        String idValue = Integer.toString(random.nextInt(0, MAX_READ_ID));
        int randTableIndex = random.nextInt(0, allTables.size());
        SaisTable selectRoot = allTables.get(randTableIndex);
        SaisTable predicateTable = choosePredicate(selectRoot, random.nextInt());
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
        String string = ServiceManagerImpl.get().getConfigurationService().getProperty("akserver.test.mt.spawncount");
        return Integer.parseInt(string);
    }
}
