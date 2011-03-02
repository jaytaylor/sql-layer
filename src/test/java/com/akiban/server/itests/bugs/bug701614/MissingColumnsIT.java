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

package com.akiban.cserver.itests.bugs.bug701580;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.itests.ApiTestBase;
import com.akiban.util.Strings;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public final class MissingColumnsIT extends ApiTestBase {
    @Test
    public void testForMissingColumns() throws InvalidOperationException, IOException {
        TableId tableId = loadBlocksTable();
        writeRows( rows(tableId) );
        expectFullRows(tableId, rows(tableId));
    }

    private TableId loadBlocksTable() throws InvalidOperationException, IOException {
        final String blocksDDL = Strings.readResource("blocks-table.ddl", getClass());
        ddl().createTable(session, "drupal", blocksDDL);
        AkibaInformationSchema ais = ddl().getAIS(session);
        assertNotNull("drupal.blocks missing from " + ais.getUserTables(), ais.getUserTable("drupal", "blocks"));
        return getTableId("drupal", "blocks");
    }

    private NewRow[] rows(TableId tableId) {
        return new NewRow[] {
                createNewRow(tableId, 1L, "user", "0", "garland", 1L, 0L, "left", 0L, 0L, 0L, "", "", -1L),
                createNewRow(tableId, 2L, "user", "1", "garland", 1L, 0L, "left", 0L, 0L, 0L, "", "", -1L),
                createNewRow(tableId, 3L, "system", "0", "garland", 1L, 10L, "footer", 0L, 0L, 0L, "", "", -1L)
        };
    }
}
