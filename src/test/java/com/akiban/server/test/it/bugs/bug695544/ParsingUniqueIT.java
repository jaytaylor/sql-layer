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

package com.akiban.server.test.it.bugs.bug695544;

import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.SchemaDefParseException;
import com.akiban.server.test.it.ITBase;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

public final class ParsingUniqueIT extends ITBase {
    private final static String SCHEMA = "sc1";
    private final static String TABLE = "tb1";
    private int tableId;

    @After
    public void tearDown() {
        tableId = -1;
    }

    @Test
    public void UNIQUE() throws InvalidOperationException {
        create( "id int key",
                "c1 int UNIQUE");
        testInserts();
        testIndex("c1");
    }

    @Test
    public void UNIQUE_KEY() throws InvalidOperationException {
        create( "id int key",
                "c1 int UNIQUE KEY");
        testInserts();
        testIndex("c1");
    }

    @Test
    public void UNIQUE_KEY_UNIQUE_UNIQUE() throws InvalidOperationException {
        create( "id int key",
                "c1 int UNIQUE KEY UNIQUE UNIQUE");
        testInserts();
        testIndex("c1");
    }

    @Test
    public void KEY_KEY_KEY_UNIQUE() throws InvalidOperationException {
        create( "id int",
                "c1 int KEY KEY KEY UNIQUE");
        testInserts();
        expectIndexes(tableId, "PRIMARY", "c1");
        expectIndexColumns(tableId, "PRIMARY", "c1");
        expectIndexColumns(tableId, "c1", "c1");
    }

    @Test
    public void UNIQUE_NOT_NULL_KEY() throws InvalidOperationException {
        create( "c0 int",
                "c1 int UNIQUE NOT NULL KEY");
        testInserts();
        expectIndexes(tableId, "PRIMARY", "c1");
        expectIndexColumns(tableId, "PRIMARY", "c1");
        expectIndexColumns(tableId, "c1", "c1");
    }

    @Test
    public void UNIQUE_PRIMARY_KEY() throws InvalidOperationException {
        create("id int",
                "c1 int UNIQUE PRIMARY KEY");
        testInserts();
        expectIndexes(tableId, "PRIMARY");
        expectIndexColumns(tableId, "PRIMARY", "c1");
    }

    @Test(expected=SchemaDefParseException.class)
    public void fail_PRIMARY() throws InvalidOperationException {
        create("id int primary");
    }

    @Test(expected=SchemaDefParseException.class)
    public void fail_PRIMARY_UNIQUE_KEY() throws InvalidOperationException {
        create("id int primary unique key");
    }

    @Test(expected=SchemaDefParseException.class)
    public void fail_twoColsWithKEY() throws InvalidOperationException {
        create("id1 int key, id2 int key");
    }

    @Test
    public void constraintUNIQUE() throws InvalidOperationException {
        create( "id int key",
                "c1 int",
                "UNIQUE (c1)");
        testInserts();
        testIndex("c1");
    }

    @Test
    public void fullerConstraintUNIQUE_INDEX() throws InvalidOperationException {
        create( "id int key",
                "c1 int",
                "CONSTRAINT my_uniqueness_constraint UNIQUE INDEX my_uniqueness_index (c1)");
        testInserts();
        testIndex("my_uniqueness_index");
    }

    @Test
    public void fullerConstraintUNIQUE_KEY() throws InvalidOperationException {
        create( "id int key",
                "c1 int",
                "CONSTRAINT my_uniqueness_constraint UNIQUE KEY my_uniqueness_index (c1)");
        testInserts();
        testIndex("my_uniqueness_index");
    }

    @Test
    public void fullerConstraintUNIQUE() throws InvalidOperationException {
        create( "id int key",
                "c1 int",
                "CONSTRAINT my_uniqueness_constraint UNIQUE my_uniqueness_index (c1)");
        testInserts();
        testIndex("my_uniqueness_index");
    }

    private void create(String... definitions) throws InvalidOperationException {
        tableId = createTable(SCHEMA, TABLE, definitions);
    }

    private void testInserts() throws InvalidOperationException {
        writeRows( createNewRow(tableId, 10, 11) );

        DuplicateKeyException expected = null;
        try {
            writeRows( createNewRow(tableId, 20, 11) );
        } catch (DuplicateKeyException e) {
            expected = e;
        }
        assertNotNull("expected a DuplicateKeyException", expected);

        expectFullRows(tableId, createNewRow(tableId, 10L, 11L));
    }

    private void testIndex(String indexName) {
        expectIndexes(tableId, "PRIMARY", indexName);
        expectIndexColumns(tableId, "PRIMARY", "id");
        expectIndexColumns(tableId, indexName, "c1");
    }
}
