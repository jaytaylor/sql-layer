
package com.akiban.server.test.it.dxl;

import com.akiban.server.test.it.ITBase;
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
