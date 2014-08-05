/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.test.it.nopk;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public final class NoPkIT extends ITBase {
    @Test
    public void replAkiban() throws Exception {
        int tableId = createTable("test", "REPL_C", "I INT");
        writeRows(
                createNewRow(tableId, 2),
                createNewRow(tableId, 2)
        );

        expectFullRows( tableId,
                // expectFullRows checks user-visible data, and so does not check the __akiban_pk column.
                createNewRow(tableId, 2),
                createNewRow(tableId, 2)
        );
    }

    // Inspired by bug 1023944
    @Test
    public void GIWithPKLessLeafTable_dropLeaf()
    {
        int t1 =
            createTable("schema", "t1",
                        "id int not null",
                        "primary key(id)");
        int t2 =
            createTable("schema", "t2",
                        "id int",
                        "ref int",
                        "grouping foreign key(id) references t1(id)");
        writeRows(
            createNewRow(t1, 1),
            createNewRow(t1, 2),
            createNewRow(t1, 3));
        writeRows( // Bug 1023945 produces NPE when writing rows to t2
                   createNewRow(t2, 1, 1),
                   createNewRow(t2, 2, 1),
                   createNewRow(t2, 3, 1));
        createLeftGroupIndex(new TableName("schema", "t1"), "gi", "t2.ref", "t2.id", "t1.id");
        ddl().dropTable(session(), new TableName("schema", "t2"));
    }

    // Inspired by bug 1023945
    @Test
    public void GIWithPKLessLeafTable_populate()
    {
        int t1 =
            createTable("schema", "t1",
                        "id int not null",
                        "primary key(id)");
        int t2 =
            createTable("schema", "t2",
                        "id int",
                        "ref int",
                        "grouping foreign key(id) references t1(id)");
        createLeftGroupIndex(new TableName("schema", "t1"), "gi", "t2.ref", "t2.id", "t1.id");
        writeRows(
            createNewRow(t1, 1),
            createNewRow(t1, 2),
            createNewRow(t1, 3));
        writeRows( // Bug 1023945 produces NPE when writing rows to t2
            createNewRow(t2, 1, 1),
            createNewRow(t2, 2, 1),
            createNewRow(t2, 3, 1));
    }
}
