package com.akiban.cserver.itests.bugs.bug696096;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.DuplicateKeyException;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.*;

public final class DuplicateKeyValueMessageTest extends ApiTestBase {
    private TableId tableId;

    @Before
    public void setUp() throws InvalidOperationException {
        assert tableId == null;
        tableId = createTable("sa", "ta",
                "c0 INT KEY",
                "c1 int",
                "c2 int",
                "c3 int",
                "name varchar(32)",
                "UNIQUE (c1, c2)",
                "UNIQUE my_key(c3)"
        );
        writeRows(
                createNewRow(tableId, 10, 11, 12, 13, "from setup"),
                createNewRow(tableId, 20, 21, 22, 23, "from setup")
        );
    }

    @After
    public void tearDown() throws InvalidOperationException {
        expectFullRows(tableId,
                createNewRow(tableId, 10L, 11L, 12L, 13L, "from setup"),
                createNewRow(tableId, 20L, 21L, 22L, 23L, "from setup")
        );
    }

    @Test
    public void writeDuplicatesPrimary() {
        duplicateOnWrite("PRIMARY", 10, 91, 92, 93);
    }

    @Test
    public void writeDuplicatesC1() {
        duplicateOnWrite("c1", 90, 11, 12, 93);
    }

    @Test
    public void writeDuplicatesMyKey() {
        duplicateOnWrite("my_key", 90, 91, 92, 13);
    }

    @Test
    public void writeDuplicatesMultiple() {
        duplicateOnWrite("PRIMARY", 10, 11, 12, 13);
    }

    @Test
    public void updateDuplicatesPrimary() {
        duplicateOnUpdate("PRIMARY", 10, 91, 92, 93);
    }

    @Test
    public void updateDuplicatesC1() {
        duplicateOnUpdate("c1", 90, 11, 12, 93);
    }

    @Test
    public void updateDuplicatesMyKey() {
        duplicateOnUpdate("my_key", 90, 91, 92, 13);
    }

    @Test
    public void updateDuplicatesMultiple() {
        duplicateOnUpdate("PRIMARY", 10, 11, 12, 13);
    }

    private static void dupMessageValid(DuplicateKeyException e, String indexName) {
        boolean messageIsValid = e.getMessage().contains(indexName);

        if (!messageIsValid) {
            String errString = String.format("expected message to contain <%s>, but was <%s>",
                    indexName, e.getMessage()
            );
            e.printStackTrace();
            fail(errString);
        }
    }

    private void duplicateOnWrite(String indexName, int c0, int c1, int c2, int c3) {
        try {
            writeRows(createNewRow(tableId, c0, c1, c2, c3, "from write"));
        } catch (DuplicateKeyException e) {
            dupMessageValid(e, indexName);
            return;
        } catch (InvalidOperationException e) {
            throw new TestException("unexpected exception", e);
        }
        fail("Excpected DuplicateKeyExcepton");
    }
    
    private void duplicateOnUpdate(String indexName, int c0, int c1, int c2, int c3) {
        try {
            NewRow oldRow = createNewRow(tableId, 20, 21, 22, 23, "from setup");
            NewRow newRow = createNewRow(tableId, c0, c1, c2, c3, "from update");
            dml().updateRow(session, oldRow, newRow);
        } catch (DuplicateKeyException e) {
            dupMessageValid(e, indexName);
            return;
        } catch (InvalidOperationException e) {
            throw new TestException("unexpected exception", e);
        }
        fail("Excpected DuplicateKeyExcepton");
    }
}
