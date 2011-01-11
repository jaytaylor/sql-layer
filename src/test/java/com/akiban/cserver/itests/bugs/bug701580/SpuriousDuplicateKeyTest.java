package com.akiban.cserver.itests.bugs.bug701580;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.itests.ApiTestBase;
import com.akiban.util.Strings;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;


public final class SpuriousDuplicateKeyTest extends ApiTestBase {

    @Test
    public void passWithConfirmation() throws Exception {
        doPass(true);
    }

    @Test
    public void onePass() throws Exception {
        doPass(false);
    }

    @Test
    public void twoPasses() throws Exception {
        doPass(false);
        doPass(false);
    }

    private void doPass(boolean confirmWrites) throws Exception {
        TableId tableId = loadBlocksTable();
        loadData(tableId, confirmWrites);
        dropBlocksTable(tableId);
    }

    private TableId loadBlocksTable() throws Exception {
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

    private void loadData(TableId tableId, boolean confirm) throws InvalidOperationException {
        writeRows(rows(tableId));
        if (confirm) {
            expectFullRows(tableId, rows(tableId));
        }
    }

    private void dropBlocksTable(TableId tableId) throws Exception {
        ddl().dropTable(session, tableId);
        Map<TableName, UserTable> uTables = ddl().getAIS(session).getUserTables();
        for(Iterator<TableName> iter=uTables.keySet().iterator(); iter.hasNext(); ) {
            if("akiba_information_schema".equals(iter.next().getSchemaName())) {
                iter.remove();
            }
        }
        assertEquals("user tables", Collections.<TableName, UserTable>emptyMap(), ddl().getAIS(session).getUserTables());
    }
}
