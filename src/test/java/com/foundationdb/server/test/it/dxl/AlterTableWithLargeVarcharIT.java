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

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.server.test.it.ITBase;
import com.google.common.base.Strings;
import org.junit.Test;

public final class AlterTableWithLargeVarcharIT extends ITBase {
    @Test
    public void reallocation() {
        int tableId = createTable("myschema", "mytable", "id INT NOT NULL PRIMARY KEY",
                "vc_col0 VARCHAR(500), vc_col1 VARCHAR(500)");
        String bigString = Strings.repeat("a", 476);
        writeRow(tableId, 1L, bigString, "hi");
        // bigString is large enough that "hi" has an offset of 499 in the RowData byte array.
        // When we add 5 null ints to the RowData, the nullmap will grow such that "hi" has an offset of 500,
        // at which point it'll need to regrow.
        for (int i = 0; i < 6; ++i) {
            runAlter("myschema", null, "alter table mytable add intcol_" + i + " INTEGER");
        }
    }
}
