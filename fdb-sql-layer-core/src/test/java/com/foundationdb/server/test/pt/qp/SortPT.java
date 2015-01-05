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

package com.foundationdb.server.test.pt.qp;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static com.foundationdb.qp.operator.API.*;

public class SortPT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        t = createTable(
            "schema", "t",
            "id int not null key",
            "rand int",
            "filler varchar(20)");
        group = group(t);
        schema = SchemaCache.globalSchema(ais());
        tRowType = schema.tableRowType(table(t));
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    @Test
    public void profileSort()
    {
        InOutTap tap = Operator.OPERATOR_TAP; // Force loading of class and registration of tap.
        Tap.setEnabled(OPERATOR_TAPS, true);
        ordering = ordering();
        ordering.append(ExpressionGenerators.field(tRowType, 0), true);
        plan = sort_General(groupScan_Default(group), tRowType, ordering, SortOption.PRESERVE_DUPLICATES);
        populateDB(10000000);
        // Warmup
        profileSort(10000, 0, false);
        profileSort(10000, 0, false);
        profileSort(10000, 1, false);
        profileSort(10000, 1, false);
        // Measure ordered input
        profileSort(1000, 0, true);
        profileSort(2500, 0, true);
        profileSort(5000, 0, true);
        profileSort(10000, 0, true);
        profileSort(25000, 0, true);
        profileSort(50000, 0, true);
        profileSort(100000, 0, true);
        profileSort(250000, 0, true);
        profileSort(500000, 0, true);
        profileSort(1000000, 0, true);
        profileSort(2500000, 0, true);
        profileSort(5000000, 0, true);
        profileSort(10000000, 0, true);
        // Measure unordered input
        profileSort(1000, 1, true);
        profileSort(2500, 1, true);
        profileSort(5000, 1, true);
        profileSort(10000, 1, true);
        profileSort(25000, 1, true);
        profileSort(50000, 1, true);
        profileSort(100000, 1, true);
        profileSort(250000, 1, true);
        profileSort(500000, 1, true);
        profileSort(1000000, 1, true);
        profileSort(2500000, 1, true);
        profileSort(5000000, 1, true);
        profileSort(10000000, 1, true);
    }
    
    public void profileSort(int n, int field, boolean print)
    {
        Tap.reset(OPERATOR_TAPS);
        Ordering ordering = ordering();
        ordering.append(ExpressionGenerators.field(tRowType, field), true);
        Operator plan = 
            sort_General(
                limit_Default(
                    groupScan_Default(group),
                    n),
                tRowType, 
                ordering, 
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        while (cursor.next() != null) {
        }
        cursor.closeTopLevel();
        if (print) {
            System.out.println("---------------------------------------------------------------------");
            System.out.println(String.format("Sort %s on field %s", n, field == 0 ? "id" : "rand"));
            TapReport[] reports = Tap.getReport(OPERATOR_TAPS);
            for (TapReport report : reports) {
                System.out.println(report);
            }
        }
    }

    private void populateDB(int n)
    {
        for (int id = 0; id < n; id++) {
            writeRow(t, id, random.nextInt(), FILLER);
        }
    }

    private static final String OPERATOR_TAPS = ".*operator.*";
    private static final String FILLER = "xxxxxxxxxxxxxxxxxxxx";

    private final Random random = new Random();
    private int t;
    private Group group;
    private Schema schema;
    private RowType tRowType;
    private StoreAdapter adapter;
    private Ordering ordering;
    private Operator plan;
}
