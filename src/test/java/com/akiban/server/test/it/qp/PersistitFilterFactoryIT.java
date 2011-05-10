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

package com.akiban.server.test.it.qp;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.NoLimit;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public final class PersistitFilterFactoryIT extends ITBase {
    @Test
    public void coiScanOnI() throws Exception {
        int cTable = createTable("schema", "customers", "cid int key");
        int oTable = createTable("schema", "orders", "oid int key, cid int",
                "CONSTRAINT __akiban_o FOREIGN KEY __akiban_o(cid) REFERENCES customers(cid)");
        int iTable = createTable("schema", "items", "iid int key, oid int",
                "CONSTRAINT __akiban_i FOREIGN KEY __akiban_i(oid) REFERENCES orders(oid)");
        Schema schema = new Schema(rowDefCache().ais());
        RowType itemRowType = schema.userTableRowType(getUserTable(iTable));
        writeRows(
                createNewRow(cTable, 1L),
                createNewRow(oTable, 10L, 1L),
                createNewRow(iTable, 100L, 10L)
        );
        Row row = new TestRow(itemRowType, objArray(100L, 10L));

        IndexBound bound = new IndexBound(getUserTable(iTable), row, new ConstantColumnSelector(true));
        IndexKeyRange range = new IndexKeyRange(bound, true, bound, true);

        PhysicalOperator groupScan = API.groupScan_Default(
                getUserTable(iTable).getGroup().getGroupTable(),
                false,
                NoLimit.instance(),
                range
        );
        Cursor groupCursor = groupScan.cursor(new PersistitAdapter(schema, persistitStore(), session()));
        groupCursor.open(UndefBindings.only());
        List<Row> rows = new ArrayList<Row>();
        while (groupCursor.next()) {
            rows.add( groupCursor.currentRow() );
        }
        groupCursor.close();

        assertEquals("rows.size()", 3, rows.size());
    }
}
