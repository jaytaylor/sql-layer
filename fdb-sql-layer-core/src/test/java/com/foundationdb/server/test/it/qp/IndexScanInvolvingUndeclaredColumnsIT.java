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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.test.ExpressionGenerators;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;

// Inspired by Bug 979162

public class IndexScanInvolvingUndeclaredColumnsIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        region = createTable(
            "schema", "region",
            "rid int not null",
            "primary key(rid)");
        regionChildren = createTable(
            "schema", "region_children",
            "rid int not null",
            "locid int not null",
            "grouping foreign key(rid) references region(rid)");
        createIndex("schema", "region_children", "idx_locid", "locid");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        regionChildrenRowType = schema.tableRowType(table(regionChildren));
        idxRowType = indexType(regionChildren, "locid");
        db = new Row[]{
            // region
            row(region, 10L),
            row(region, 20L),
            // region_children (last column is hidden PK)
            row(regionChildren, 10L, 100L, 1L),
            row(regionChildren, 10L, 110L, 2L),
            row(regionChildren, 10L, 120L, 3L),
            row(regionChildren, 20L, 200L, 4L),
            row(regionChildren, 20L, 210L, 5L),
            row(regionChildren, 20L, 220L, 6L),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    @Test
    public void test()
    {
        IndexBound bound = new IndexBound(row(idxRowType, 110L, 15L),
                                          new SetColumnSelector(0, 1));
        IndexKeyRange range = IndexKeyRange.bounded(idxRowType, bound, true, bound, true);
        API.Ordering ordering = new API.Ordering();
        ordering.append(ExpressionGenerators.field(idxRowType, 0), true);
        ordering.append(ExpressionGenerators.field(idxRowType, 1), true);
        Operator plan =
            indexScan_Default(
                idxRowType,
                range,
                ordering);
        compareRows(new Row[0], cursor(plan, queryContext, queryBindings));
    }

    // For use by this class

    private API.Ordering ordering(Object ... ord) // alternating column positions and asc/desc
    {
        API.Ordering ordering = API.ordering();
        int i = 0;
        while (i < ord.length) {
            int column = (Integer) ord[i++];
            boolean asc = (Boolean) ord[i++];
            ordering.append(field(idxRowType, column), asc);
        }
        return ordering;
    }

    private int region;
    private int regionChildren;
    private RowType regionChildrenRowType;
    private IndexRowType idxRowType;
}
