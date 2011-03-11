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
import com.akiban.server.api.ApiTest;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.NoSuchIndexException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.mthapi.base.HapiMTBase;
import com.akiban.server.mttests.mthapi.base.HapiReadThread;
import com.akiban.server.mttests.mthapi.base.HapiSuccess;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.mttests.mthapi.base.WriteThreadStats;
import com.akiban.server.mttests.mthapi.base.sais.SaisBuilder;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import com.akiban.server.mttests.mthapi.common.BasicHapiSuccess;
import com.akiban.server.service.session.Session;
import com.sun.java.help.search.Schema;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public final class AddDropIndexMT extends HapiMTBase {
    private static final String SCHEMA = "indexestest";
    @Test
    public void addDropIndex() {
        WriteThread writeThread = getAddDropIndexThread("theindex");
        HapiReadThread readThread = readThread();
        runThreads(writeThread, readThread);
    }

    private HapiReadThread readThread() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("p", "id", "aString", "anInt").pk("id");
        builder.table("c1", "id", "pid").pk("id").joinTo("p").col("id", "pid");
        SaisTable pTable = builder.getSoleRootTable();
        return new BasicHapiSuccess(SCHEMA, pTable) {
            @Override
            protected void validateErrorResponse(HapiGetRequest request, Throwable exception)
                    throws UnexpectedException
            {
                if (exception instanceof HapiRequestException) {
                    HapiRequestException hre = (HapiRequestException) exception;
                    assertEquals("HapiRequestException cause",
                            HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST,
                            hre.getReasonCode()
                    );
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

            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                    throws InvalidOperationException
            {
                boolean shouldCreate = true;
                boolean createForString = true;
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
            public WriteThreadStats getStats() {
                return new WriteThreadStats(0, 0, 0);
            }
        };
    }
}
