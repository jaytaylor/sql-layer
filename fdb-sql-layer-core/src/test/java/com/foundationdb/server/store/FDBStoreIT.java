/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store;

import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.it.FDBITBase;
import org.junit.Test;

import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class FDBStoreIT extends FDBITBase
{
    private static final String SCHEMA = "test";

    private void nextSequenceValue(final TableName seqName) {
        txnService().run(session(), new Runnable()  {
            @Override
            public void run() {
                Sequence s = ais().getSequence(seqName);
                fdbStore().nextSequenceValue(session(), s);
            }
        });
    }

    @Test
    public void dropSequenceMaintainsSequenceCache() {
        int initial = fdbStore().getSequenceCacheMapSize();
        TableName seqName = new TableName(SCHEMA, "s");
        createSequence(SCHEMA, seqName.getTableName(), "");
        nextSequenceValue(seqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));
        ddl().dropSequence(session(), seqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), lessThanOrEqualTo(initial));
    }

    @Test
    public void dropTableWithSerialMaintainsSequenceCache() {
        int initial = fdbStore().getSequenceCacheMapSize();
        TableName tableName = new TableName(SCHEMA, "t");
        createTable(SCHEMA, tableName.getTableName(), "id SERIAL NOT NULL PRIMARY KEY");
        TableName serialSeqName = ais().getTable(tableName).getColumn("id").getIdentityGenerator().getSequenceName();
        nextSequenceValue(serialSeqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));
        ddl().dropTable(session(), tableName);
        assertThat(fdbStore().getSequenceCacheMapSize(), lessThanOrEqualTo(initial));
    }

    @Test
    public void dropSchemaMaintainsSequenceCache() {
        int initial = fdbStore().getSequenceCacheMapSize();

        TableName seqName = new TableName(SCHEMA, "s");
        createSequence(SCHEMA, seqName.getTableName(), "");
        nextSequenceValue(seqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));

        TableName tableName = new TableName(SCHEMA, "t");
        createTable(SCHEMA, tableName.getTableName(), "id SERIAL NOT NULL PRIMARY KEY");
        TableName identitySeqName = ais().getTable(tableName).getColumn("id").getIdentityGenerator().getSequenceName();
        nextSequenceValue(identitySeqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));

        ddl().dropSchema(session(), SCHEMA);
        assertThat(fdbStore().getSequenceCacheMapSize(), lessThanOrEqualTo(initial));
    }

    @Test
    public void dropNonSystemSchemasMaintainsSequenceCache() {
        int initial = fdbStore().getSequenceCacheMapSize();

        for(String schema : new String[] { "test1", "test2" }) {
            TableName seqName = new TableName(schema, "s");
            createSequence(schema, seqName.getTableName(), "");
            nextSequenceValue(seqName);
            assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));

            TableName tableName = new TableName(schema, "t");
            createTable(schema, tableName.getTableName(), "id SERIAL NOT NULL PRIMARY KEY");
            TableName identitySeqName = ais().getTable(tableName).getColumn("id").getIdentityGenerator().getSequenceName();
            nextSequenceValue(identitySeqName);
            assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));
        }

        ddl().dropNonSystemSchemas(session());
        assertThat(fdbStore().getSequenceCacheMapSize(), lessThanOrEqualTo(initial));
    }
}