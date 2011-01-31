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

package com.akiban.cserver.itests.nopk;

import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

public final class NoPkIT extends ApiTestBase {
    @Test
    public void replAkiban() throws Exception {
        int tableId = createTable("test", "REPL_C", "I INT");
        writeRows(
                // REPL_C is a no-PK table, so it has an Akiban-supplied __akiban_pk column, (guaranteed to be the
                // last column). Need to provide a non-null value. This value is ignored, but it must be non-null.
                createNewRow(tableId, 2L, -1L),
                createNewRow(tableId, 2L, -1L)
        );

        expectFullRows( tableId,
                // expectFullRows checks user-visible data, and so does not check the __akiban_pk column.
                createNewRow(tableId, 2L),
                createNewRow(tableId, 2L)
        );
    }
}