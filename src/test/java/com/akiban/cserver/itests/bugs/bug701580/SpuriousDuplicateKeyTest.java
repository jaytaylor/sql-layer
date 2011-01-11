package com.akiban.cserver.itests.bugs.bug701580;

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

    @org.junit.Ignore @Test // see bug701614
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

    @Test
    public void loadAllThenTest() throws Exception {
        loadAllTables();
        TableId blocksTable1 = getTableId("drupal", "blocks");
        loadData(blocksTable1, false);
        dropAllTables();

        loadAllTables();
        TableId blocksTable2 = getTableId("drupal", "blocks");
        loadData(blocksTable2, false);
        dropAllTables();
    }

    private void doPass(boolean confirmWrites) throws Exception {
        loadTable("batch");
        TableId tableId = loadTable("blocks");
        loadData(tableId, confirmWrites);
        dropAllTables();
    }

    private TableId loadTable(String which) throws Exception {
        final String blocksDDL = Strings.readResource(which + "-table.ddl", getClass());
        ddl().createTable(session, "drupal", blocksDDL);
        return getTableId("drupal", which);
    }

    private void loadAllTables() throws Exception {
        createTablesFromResource("blocks-table.ddl", "drupal");
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
}
