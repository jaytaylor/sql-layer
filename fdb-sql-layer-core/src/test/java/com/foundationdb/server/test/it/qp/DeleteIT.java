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
import static com.foundationdb.qp.operator.API.delete_Returning;
import static com.foundationdb.qp.operator.API.filter_Default;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertEquals;

public class DeleteIT extends OperatorITBase {
    @Test
    public void deleteCustomer() {
        use(db);
        doDelete();
        compareRows(
                array(Row.class,
                      row(customerRowType, 1L, "xyz")
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
    public void deleteCustomerCheckNameIndex() {
        use(db);
        doDelete();
        compareRows(
                array(Row.class,
                      row(customerNameIndexRowType, "xyz", 1L)
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
    public void deleteCustomerCheckNameItemOidGroupIndex() {
        use(db);
        doDelete();
        compareRows(
                array(Row.class,
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 111L),
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 112L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 121L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 122L)
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

    private void doDelete() {
        Row[] rows = {
                row(customerRowType, new Object[]{2, "abc"})
        };
        Operator deletePlan = delete_Returning(rowsToValueScan(rows), false);
        List<Row> result = runPlan(queryContext, queryBindings, deletePlan);
        assertEquals("rows touched", rows.length, result.size());
    }
}
