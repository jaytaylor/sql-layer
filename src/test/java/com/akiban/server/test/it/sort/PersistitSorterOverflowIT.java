/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.test.it.sort;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.TempVolume;
import com.akiban.qp.persistitadapter.indexcursor.PersistitSorter;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.test.ExpressionGenerators;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import com.persistit.exception.VolumeFullException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PersistitSorterOverflowIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";

    @Override
    public Map<String,String> startupConfigProperties() {
        Map<String,String> props = new HashMap<>();
        props.putAll(super.startupConfigProperties());
        props.put("persistit.tmpvolmaxsize","100K");
        return props;
    }

    @Test
    public void tempVolumeFull() {
        int tid = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, v VARCHAR(32)");
        boolean caught = false;
        for(int i = 0; i < 100 && !caught; ++i) {
            for(int j = 0; j < 1000; ++j) {
                writeRow(tid, (i*1000 + j), "test");
            }
            try {
                doSort();
            } catch(PersistitAdapterException e) {
                assertEquals("caused by", VolumeFullException.class, e.getCause().getClass());
                caught = true;
            }
            String msg = "no temp state" + (caught ? " after exception" : "");
            assertEquals(msg, false, TempVolume.hasTempState(session()));
        }
        assertEquals("caught exception", true, caught);
    }

    private void doSort() {
        InOutTap tap = Tap.createTimer("test");
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        PersistitAdapter adapter = persistitAdapter(schema);

        UserTable userTable = getUserTable(SCHEMA, TABLE);
        RowType rowType = schema.userTableRowType(userTable);
        API.Ordering ordering = API.ordering();
        ordering.append(ExpressionGenerators.field(rowType, 1), true);

        QueryContext context = queryContext(adapter);
        Cursor inputCursor = API.cursor(
                API.groupScan_Default(userTable.getGroup()),
                context
        );

        inputCursor.open();
        try {
            PersistitSorter sorter = new PersistitSorter(context, inputCursor, rowType, ordering, API.SortOption.PRESERVE_DUPLICATES, tap);
            Cursor sortedCursor = sorter.sort();
            sortedCursor.open();
            try {
                while(sortedCursor.next() != null) {
                    // None
                }
            } finally {
                sortedCursor.close();
            }
        } finally {
            inputCursor.close();
        }
    }
}
