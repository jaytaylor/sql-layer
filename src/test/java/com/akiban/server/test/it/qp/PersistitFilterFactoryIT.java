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
import com.akiban.qp.persistitadapter.HookablePersistitAdapter;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.NoLimit;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.SchemaAISBased;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.test.it.ITBase;
import com.persistit.KeyFilter;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public final class PersistitFilterFactoryIT extends ITBase {

    private static class RememberingFilterFactoryHook implements HookablePersistitAdapter.FilterFactoryHook {
        private final List<KeyFilter> list = new ArrayList<KeyFilter>();

        @Override
        public void reportKeyFilter(KeyFilter keyFilter) {
            list.add(keyFilter);
        }

        public List<KeyFilter> list() {
            return list;
        }
    }

    @Ignore("780614")
    @Test
    public void coiScanOnI() throws Exception {
        RememberingFilterFactoryHook hook = new RememberingFilterFactoryHook();
        int cTable = createTable("schema", "customers", "cid int key");
        int oTable = createTable("schema", "orders", "oid int key, cid int",
                "CONSTRAINT __akiban_o FOREIGN KEY __akiban_o(cid) REFERENCES customers(cid)");
        int iTable = createTable("schema", "items", "iid int key, oid int",
                "CONSTRAINT __akiban_i FOREIGN KEY __akiban_i(oid) REFERENCES orders(oid)");
        Schema schema = new SchemaAISBased(rowDefCache().ais());
        RowType itemRowType = schema.userTableRowType(getUserTable(iTable));
        writeRows(
                createNewRow(cTable, 1L),
                createNewRow(oTable, 10L, 1L),
                createNewRow(iTable, 100L, 10L)
        );
        Row row = new TestRow(itemRowType, objArray(100L, 10L));

        IndexBound bound = new IndexBound(getUserTable(iTable), row, ConstantColumnSelector.ALL_ON);
        IndexKeyRange range = new IndexKeyRange(bound, true, bound, true);

        PhysicalOperator groupScan = API.groupScan_Default(
                getUserTable(iTable).getGroup().getGroupTable(),
                NoLimit.instance(),
                range
        );
        Cursor groupCursor = API.cursor(
                groupScan,
                new HookablePersistitAdapter(schema, persistitStore(), session(), hook)
        );
        groupCursor.open(UndefBindings.only());

        List<KeyFilter> filters = hook.list();
        assertEquals("key filters generated", 1, filters.size());
        KeyFilter keyFilter = filters.get(0);
        assertSame("second term", KeyFilter.ALL, keyFilter.getTerm(1));

        List<Row> rows = new ArrayList<Row>();
        while (groupCursor.booleanNext()) {
            rows.add( groupCursor.currentRow() );
        }
        groupCursor.close();

        assertEquals("rows.size()", 3, rows.size());
    }
}
