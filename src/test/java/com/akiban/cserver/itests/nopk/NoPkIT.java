package com.akiban.cserver.itests.nopk;

import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

public final class NoPkIT extends ApiTestBase {
    @Test
    public void replAkiban() throws Exception {
        TableId tableId = createTable("test", "REPL_C", "I INT");
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