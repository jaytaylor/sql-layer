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

package com.akiban.server.test.it.qp;

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.delete_Default;
import static com.akiban.qp.operator.API.filter_Default;
import static com.akiban.qp.operator.API.groupScan_Default;
import static com.akiban.qp.operator.API.indexScan_Default;
import static com.akiban.qp.operator.API.insert_Default;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DeleteIT extends OperatorITBase {
    @Test
    public void deleteCustomer() {
        use(db);
        doInsert();
        compareRows(
                array(TestRow.class,
                      row(customerRowType, 1L, "xyz")
                ),
                cursor(
                        filter_Default(
                                groupScan_Default(coi),
                                Collections.singleton(customerRowType)),
                        queryContext
                )
        );
    }

    @Test
    public void deleteCustomerCheckNameIndex() {
        use(db);
        doInsert();
        compareRows(
                array(RowBase.class,
                      row(customerNameIndexRowType, "xyz", 1L)
                      ),
                cursor(
                indexScan_Default(
                        customerNameIndexRowType,
                        IndexKeyRange.unbounded(customerNameIndexRowType),
                        new API.Ordering()),
                queryContext
        ));
    }

    @Test
    public void deleteCustomerCheckNameItemOidGroupIndex() {
        use(db);
        doInsert();
        compareRows(
                array(RowBase.class,
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
                queryContext
        ));
    }

    Operator rowsToValueScan(Row... rows) {
        List<BindableRow> bindableRows = new ArrayList<BindableRow>();
        RowType type = null;
        for(Row row : rows) {
            RowType newType = row.rowType();
            if(type == null) {
                type = newType;
            } else if(type != newType) {
                fail("Multiple row types: " + type + " vs " + newType);
            }
            bindableRows.add(BindableRow.of(row));
        }
        return API.valuesScan_Default(bindableRows, type);
    }

    private void doInsert() {
        Row[] rows = {
                row(customerRowType, 2, "abc"),
        };
        UpdatePlannable insertPlan = delete_Default(rowsToValueScan(rows));
        UpdateResult result = insertPlan.run(queryContext);
        assertEquals("rows touched", rows.length, result.rowsTouched());
        assertEquals("rows modified", rows.length, result.rowsModified());
    }
}