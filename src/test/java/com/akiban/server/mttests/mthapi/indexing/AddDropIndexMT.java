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

package com.akiban.server.mttests.mthapi.indexing;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.hapi.DefaultHapiGetRequest;
import com.akiban.server.mttests.mthapi.base.HapiMTBase;
import com.akiban.server.mttests.mthapi.base.HapiReadThread;
import com.akiban.server.mttests.mthapi.base.HapiRequestStruct;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.mttests.mthapi.base.WriteThreadStats;
import com.akiban.server.mttests.mthapi.base.sais.SaisBuilder;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import com.akiban.server.mttests.mthapi.common.BasicHapiSuccess;
import com.akiban.server.mttests.mthapi.common.HapiValidationError;
import com.akiban.server.service.session.Session;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AddDropIndexMT extends HapiMTBase {
    private static final String SCHEMA = "indexestest";
    @Test
    public void addDropIndex() {
        WriteThread writeThread = getAddDropIndexThread("theindex");

        runThreads(writeThread,
                readThread("aString", 200, false, .4f),
                readThread("anInt", 200, true, .4f),
                readThread("id", 100, false, .2f)
        );
    }

    private HapiReadThread readThread(final String column, final int max, final boolean reverse, final float chance) {
        SaisBuilder builder = new SaisBuilder();
        builder.table("p", "id", "aString", "anInt").pk("id");
        builder.table("c1", "id", "pid").pk("id").joinTo("p").col("id", "pid");
        final SaisTable pTable = builder.getSoleRootTable();
        return new BasicHapiSuccess(SCHEMA, pTable) {

            @Override
            protected HapiRequestStruct pullRequest(int pseudoRandom) {
                int id = (Math.abs(pseudoRandom) % (max-1)) + 1;
                if (reverse)  {
                    id = -id;
                }
                HapiGetRequest request = DefaultHapiGetRequest.forTables(SCHEMA, "p", "p")
                        .withEqualities(column, Integer.toString(id)).done();
                return new HapiRequestStruct(request, pTable, null);
            }

            @Override
            protected void validateSuccessResponse(HapiRequestStruct requestStruct, JSONObject result) throws JSONException {
                super.validateSuccessResponse(requestStruct, result);
                HapiValidationError.assertFalse(HapiValidationError.Reason.ROOT_TABLES_COUNT,
                        "more than one root found",
                        result.getJSONArray("@p").length() > 1);
                // Also, we must have results!
//                TODO: this isn't a valid test while we allow concurrent scans and adding/dropping of indexes
//                see: https://answers.launchpad.net/akiban-server/+question/148857
//                HapiValidationError.assertEquals(HapiValidationError.Reason.ROOT_TABLES_COUNT,
//                        "number of roots",
//                        1, result.getJSONArray("@p").length()
//                );
            }

            @Override
            protected int spawnCount() {
                float spawnRoughly = chance * super.spawnCount();
                return (int)(spawnRoughly + .5);
            }

            @Override
            protected void validateErrorResponse(HapiGetRequest request, Throwable exception)
                    throws UnexpectedException
            {
                if (exception instanceof HapiRequestException) {
                    HapiRequestException hre = (HapiRequestException) exception;
                    if (HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST.equals(hre.getReasonCode())) {
                        return;
                    }
                    return;
                }
                super.validateErrorResponse(request, exception);
            }
        };
    }

    private WriteThread getAddDropIndexThread(final String indexName) {
        return new WriteThread() {
            @Override
            public void setupWrites(DDLFunctions ddl, DMLFunctions dml, Session session)
                    throws InvalidOperationException
            {
                ddl.createTable(session, SCHEMA,
                        "create table p(id int key, aString varchar(32), anInt int)");
                ddl.createTable(session, SCHEMA,
                        "create table c1(id int key, pid int, "
                        + "CONSTRAINT __akiban_c1 FOREIGN KEY __akiban_c1 (pid) REFERENCES p(id) )"
                        );
                int pId = ddl.getTableId(session, new TableName(SCHEMA, "p"));
                int cId = ddl.getTableId(session, new TableName(SCHEMA, "c1"));

                int childId = 1;
                for (int parentId = 1; parentId <= 100; ++parentId) {
                    int age = parentId * 2 - 1;
                    NewRow parentRow = createNewRow(pId, parentId, Integer.toString(age), -age);
                    dml.writeRow(session, parentRow);
                    for (int childCount = 0; childCount < 5; ++childCount) {
                        NewRow childRow = createNewRow(cId, childId++, parentId);
                        dml.writeRow(session, childRow);
                    }
                }
            }

            private boolean shouldCreate;

            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                    throws InvalidOperationException
            {
                final TableName tableName = new TableName(SCHEMA, "p");

                UserTable parentTable = ddl.getUserTable(session, tableName);

                Index stringIndex = new Index(parentTable, indexName, 2, false, "KEY");
                stringIndex.addColumn(
                        new IndexColumn(stringIndex, parentTable.getColumn("aString"), 0, true, null)
                );
                Index numIndex = new Index(parentTable, indexName, 2, false, "KEY");
                numIndex.addColumn(
                        new IndexColumn(numIndex, parentTable.getColumn("anInt"), 0, true, null)
                );

                Collection<Index> stringIndexCollection = Collections.singleton(stringIndex);
                Collection<Index> numIndexCollection = Collections.singleton(numIndex);
                Collection<String> indexNameCollection = Collections.singleton(indexName);

                shouldCreate = parentTable.getIndex(indexName) == null;
                boolean createForString = true;

                while(keepGoing.get()) {
                    if (shouldCreate) {
                        Collection<Index> which = createForString ? stringIndexCollection : numIndexCollection;
                        ddl.createIndexes(session, which);
                        createForString = ! createForString;
                    }
                    else {
                        ddl.dropIndexes(session, tableName, indexNameCollection);
                    }
                    shouldCreate = ! shouldCreate;
                }
            }

            @Override
            public boolean continueThroughException(Throwable throwable) {
                return true;
            }

            @Override
            public WriteThreadStats getStats() {
                return new WriteThreadStats(0, 0, 0);
            }
        };
    }
}
