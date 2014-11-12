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

package com.foundationdb.server.test.it.pstraverse;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.test.it.keyupdate.CollectingIndexKeyVisitor;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class KeysBase extends ITBase {
    private int customers;
    private int orders;
    private int items;

    protected abstract String ordersPK();
    protected abstract String itemsPK();

    @Before
    public void setUp() throws Exception {
        String schema = "cascading";
        customers = createTable(schema, "customers", "cid bigint not null primary key");
        orders = createTable(schema, "orders",
                "cid bigint not null",
                "oid bigint not null",
                "PRIMARY KEY("+ordersPK()+")",
                "GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)"
        );
        items = createTable(schema, "items",
                "cid bigint not null",
                "oid bigint not null",
                "iid bigint not null",
                "PRIMARY KEY("+itemsPK()+")",
                "GROUPING FOREIGN KEY ("+ordersPK()+") REFERENCES orders("+ordersPK()+")"
        );

        writeRows(
                row(customers, 71L),
                row(orders, 71L, 81L),
                row(items, 71L, 81L, 91L),
                row(items, 71L, 81L, 92L),
                row(orders, 72L, 82L),
                row(items, 72L, 82L, 93L)

        );
    }

    protected int customers() {
        return customers;
    }

    protected int orders() {
        return orders;
    }

    protected int items() {
        return items;
    }

    @Test // (expected=IllegalArgumentException.class) @SuppressWarnings("unused") // junit will invoke
    public void traverseCustomersPK() throws Exception {
        traversePK(
                customers(),
                Arrays.asList(71L)
        );
    }

    @Test @SuppressWarnings(value={"unused", "unchecked"}) // junit will invoke
    public void traverseOrdersPK() throws Exception {
        traversePK(
                orders(),
                Arrays.asList(81L, 71L),
                Arrays.asList(82L, 72L)
        );
    }

    @Test @SuppressWarnings(value={"unused", "unchecked"}) // junit will invoke
    public void traverseItemsPK() throws Exception {
        traversePK(
                items(),
                Arrays.asList(91L, 71L, 81L),
                Arrays.asList(92L, 71L, 81L),
                Arrays.asList(93L, 72L, 82L)
        );
    }

    protected void traversePK(int rowDefId, List<?>... expectedIndexes) throws Exception {
        Index pkIndex = getRowDef(rowDefId).getPKIndex();

        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            CollectingIndexKeyVisitor visitor = new CollectingIndexKeyVisitor();
            store().traverse(session(), pkIndex, visitor, -1, 0);
            assertEquals("traversed indexes", Arrays.asList(expectedIndexes), visitor.records());
            txn.commit();
        }
    }

    @Test
    public void scanCustomers() throws InvalidOperationException {
        expectRows(
                customers,
                row(customers, 71L)
        );
    }

    @Test
    public void scanOrders() throws InvalidOperationException {
        expectRows(
                orders,
                row(orders, 71L, 81L),
                row(orders, 72L, 82L)
        );
    }

    @Test
    public void scanItems() throws InvalidOperationException {
        expectRows(
                items,
                row(items, 71L, 81L, 91L),
                row(items, 71L, 81L, 92L),
                row(items, 72L, 82L, 93L)
        );
    }
}
