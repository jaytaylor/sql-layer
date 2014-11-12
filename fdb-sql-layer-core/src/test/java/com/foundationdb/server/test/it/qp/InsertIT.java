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

import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.filter_Default;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.qp.operator.API.insert_Returning;
import static org.junit.Assert.assertEquals;

public class InsertIT extends OperatorITBase {
    @Test
    public void insertCustomer() {
        use(db);
        doInsert();
        compareRows(
                array(Row.class,
                      row(customerRowType, 0L, "zzz"),
                      row(customerRowType, 1L, "xyz"),
                      row(customerRowType, 2L, "abc"),
                      row(customerRowType, 3L, "jkl"),
                      row(customerRowType, 5L, "ooo")
                ),
                cursor(
                        filter_Default(
                                groupScan_Default(coi),
                                Collections.singleton(customerRowType)),
                        queryContext, queryBindings
                )
        );
    }

    @Test
    public void insertCustomerCheckNameIndex() {
        use(db);
        doInsert();
        compareRows(
                array(Row.class,
                      row(customerNameIndexRowType, "abc", 2L),
                      row(customerNameIndexRowType, "jkl", 3L),
                      row(customerNameIndexRowType, "ooo", 5L),
                      row(customerNameIndexRowType, "xyz", 1L),
                      row(customerNameIndexRowType, "zzz", 0L)
                      ),
                cursor(
                indexScan_Default(
                        customerNameIndexRowType,
                        IndexKeyRange.unbounded(customerNameIndexRowType),
                        new API.Ordering()),
                queryContext, queryBindings
        ));
    }

    @Test
    public void insertCustomerCheckNameItemOidGroupIndex() {
        use(db);
        doInsert();
        compareRows(
                array(Row.class,
                      row(customerNameItemOidIndexRowType, "abc", 21L, 2L, 21L, 211L),
                      row(customerNameItemOidIndexRowType, "abc", 21L, 2L, 21L, 212L),
                      row(customerNameItemOidIndexRowType, "abc", 22L, 2L, 22L, 221L),
                      row(customerNameItemOidIndexRowType, "abc", 22L, 2L, 22L, 222L),
                      row(customerNameItemOidIndexRowType, "jkl", null, 3L, null, null),
                      row(customerNameItemOidIndexRowType, "ooo", null, 5L, null, null),
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 111L),
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 112L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 121L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 122L),
                      row(customerNameItemOidIndexRowType, "zzz", null, 0L, null, null)
                ),
                cursor(
                indexScan_Default(
                        customerNameItemOidIndexRowType,
                        IndexKeyRange.unbounded(customerNameItemOidIndexRowType),
                        new API.Ordering(),
                        customerRowType),
                queryContext, queryBindings
        ));
    }

    private void doInsert() {
        Row[] rows = {
                row(customerRowType, new Object[]{0, "zzz"}),
                row(customerRowType, new Object[]{3, "jkl"}),
                row(customerRowType, new Object[]{5, "ooo"})
        };
        Operator insertPlan = insert_Returning(rowsToValueScan(rows));
        List<Row> result = runPlan(queryContext, queryBindings, insertPlan);
        assertEquals("rows touched", rows.length, result.size());
    }
}
