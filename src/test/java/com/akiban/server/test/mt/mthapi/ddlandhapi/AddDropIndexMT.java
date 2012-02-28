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

package com.akiban.server.test.mt.mthapi.ddlandhapi;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.hapi.DefaultHapiGetRequest;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.OldAISException;
import com.akiban.server.error.TableDefinitionChangedException;
import com.akiban.server.test.mt.mthapi.base.HapiMTBase;
import com.akiban.server.test.mt.mthapi.base.HapiReadThread;
import com.akiban.server.test.mt.mthapi.base.HapiRequestStruct;
import com.akiban.server.test.mt.mthapi.base.WriteThread;
import com.akiban.server.test.mt.mthapi.base.sais.SaisBuilder;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import com.akiban.server.service.session.Session;
import com.akiban.util.ThreadlessRandom;
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

    private HapiReadThread readThread(final String column, final int max, final boolean reverse, float chance) {
        SaisBuilder builder = new SaisBuilder();
        builder.table("p", "id", "aString", "anInt").pk("id");
        builder.table("c1", "id", "pid").pk("id").joinTo("p").col("id", "pid");
        final SaisTable pTable = builder.getSoleRootTable();
        return new OptionallyWorkingReadThread(SCHEMA, pTable, chance, HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST) {

            @Override
            protected void validateErrorResponse(HapiGetRequest request, Throwable exception) throws UnexpectedException {
                if (exception instanceof HapiRequestException) {
                    Throwable cause = exception.getCause();
                    if (cause != null && (
                            cause.getClass().equals(OldAISException.class)
                            || cause.getClass().equals(TableDefinitionChangedException.class))
                    ) {
                        return; // expected
                    }
                }
                super.validateErrorResponse(request, exception);
            }

            @Override
            protected HapiRequestStruct pullRequest(ThreadlessRandom random) {
                int id = random.nextInt(0, max-1) + 1;
                if (reverse)  {
                    id = -id;
                }
                HapiGetRequest request = DefaultHapiGetRequest.forTables(SCHEMA, "p", "p")
                        .withEqualities(column, Integer.toString(id)).done();
                return new HapiRequestStruct(request, pTable, null);
            }

        };
    }

    private WriteThread getAddDropIndexThread(final String indexName) {
        return new WriteThread() {
            @Override
            public void setupWrites(DDLFunctions ddl, DMLFunctions dml, Session session)
                    throws InvalidOperationException
            {
                NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
                builder.userTable("p").colLong("id", false).colString("aString", 32).colLong("anInt").pk("id");
                builder.userTable("c1").colLong("id", false).colLong("pid").pk("id").joinTo("p").on("pid", "id");
                AkibanInformationSchema tempAIS = builder.ais();

                ddl.createTable(session, tempAIS.getUserTable(SCHEMA, "p"));
                ddl.createTable(session, tempAIS.getUserTable(SCHEMA, "c1"));
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

                Index stringIndex = new TableIndex(parentTable, indexName, 2, false, "KEY");
                stringIndex.addColumn(
                        new IndexColumn(stringIndex, parentTable.getColumn("aString"), 0, true, null)
                );
                Index numIndex = new TableIndex(parentTable, indexName, 2, false, "KEY");
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
                        ddl.dropTableIndexes(session, tableName, indexNameCollection);
                    }
                    shouldCreate = ! shouldCreate;
                }
            }

            @Override
            public boolean continueThroughException(Throwable throwable) {
                return true;
            }
        };
    }
}
