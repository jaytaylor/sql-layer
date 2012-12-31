/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.FixedCountLimit;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.api.dml.scan.WrappingRowOutput;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.GrowableByteBuffer;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public final class ScanBufferTooSmallIT extends ITBase {

    @Before
    public void createTables() throws InvalidOperationException {
        int cid = createTable("ts", "c",
                "cid int not null primary key",
                "name varchar(255)");
        int oid = createTable("ts", "o",
                "oid int not null primary key",
                "cid int",
                "GROUPING FOREIGN KEY (cid) REFERENCES c(cid)");
        int iid = createTable("ts", "i",
                "iid int not null primary key",
                "oid int",
                "GROUPING FOREIGN KEY (oid) REFERENCES o(oid)");

        writeRows(
                createNewRow(cid, 1, "short name"),
                createNewRow(oid, 1, 1),
                createNewRow(iid, 1, 1),
                createNewRow(iid, 2, 1),

                createNewRow(cid, 2, "this name is much longer than the previous name, which was short")
        );
    }

    @Test(expected=BufferFullException.class)
    public void onUserTable() throws InvalidOperationException, BufferFullException {
        UserTable userTable = getUserTable("ts", "c");
        doTest(userTable, userTable.getPrimaryKey().getIndex().getIndexId());
    }

    private void doTest(Table table, int indexId) throws InvalidOperationException, BufferFullException {
        Set<Integer> columns = allColumns(table);
        int size = sizeForOneRow(table.getTableId(), indexId, columns);
        LegacyRowOutput tooSmallOutput = new WrappingRowOutput( new GrowableByteBuffer(size) );
        tooSmallOutput.getOutputBuffer().mark();

        ScanRequest request = new ScanAllRequest(
                table.getTableId(), columns, indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        CursorId cursorId = dml().openCursor(session(), aisGeneration(), request);
        try {
            dml().scanSome(session(), cursorId, tooSmallOutput);
        } finally {
            dml().closeCursor(session(), cursorId);
        }
    }

    private Set<Integer> allColumns(Table table) {
        Set<Integer> cols = new HashSet<Integer>();
        int colsCount = table.getColumns().size();
        while (--colsCount >= 0) {
            cols.add(colsCount);
        }
        return cols;
    }

    private int sizeForOneRow(int tableId, int indexId, Set<Integer> columns) throws InvalidOperationException {
        LegacyRowOutput output = new WrappingRowOutput( new GrowableByteBuffer(1024) ); // plenty of space!
        output.getOutputBuffer().mark();

        ScanRequest request = new ScanAllRequest(
                tableId, columns, indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END),
                new FixedCountLimit(1)
        );
        CursorId cursorId = dml().openCursor(session(), aisGeneration(), request);
        try {
            dml().scanSome(session(), cursorId, output);
        } catch (BufferFullException e) {
            throw new RuntimeException(e);
        } finally {
            dml().closeCursor(session(), cursorId);
        }

        return output.getOutputBuffer().position();
    }
}
