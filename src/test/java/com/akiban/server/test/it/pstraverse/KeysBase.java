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

package com.akiban.server.test.it.pstraverse;

import com.akiban.ais.model.Index;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.test.it.keyupdate.CollectingIndexKeyVisitor;
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
        customers = createTable(schema, "customers", "cid int not null primary key");
        orders = createTable(schema, "orders",
                "cid int not null",
                "oid int not null",
                "PRIMARY KEY("+ordersPK()+")",
                "GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)"
        );
        items = createTable(schema, "items",
                "cid int not null",
                "oid int not null",
                "iid int not null",
                "PRIMARY KEY("+itemsPK()+")",
                "GROUPING FOREIGN KEY ("+ordersPK()+") REFERENCES orders("+ordersPK()+")"
        );

        writeRows(
                createNewRow(customers, 71),
                createNewRow(orders, 71, 81),
                createNewRow(items, 71, 81, 91),
                createNewRow(items, 71, 81, 92),
                createNewRow(orders, 72, 82),
                createNewRow(items, 72, 82, 93)

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

    @Test @SuppressWarnings("unused") // junit will invoke
    public void traverseOrdersPK() throws Exception {
        traversePK(
                orders(),
                Arrays.asList(81L, 71L),
                Arrays.asList(82L, 72L)
        );
    }

    @Test @SuppressWarnings("unused") // junit will invoke
    public void traverseItemsPK() throws Exception {
        traversePK(
                items(),
                Arrays.asList(91L, 71L, 81L),
                Arrays.asList(92L, 71L, 81L),
                Arrays.asList(93L, 72L, 82L)
        );
    }

    protected void traversePK(int rowDefId, List<? super Long>... expectedIndexes) throws Exception {
        Index pkIndex = getRowDef(rowDefId).getPKIndex();

        CollectingIndexKeyVisitor visitor = new CollectingIndexKeyVisitor();
        persistitStore().traverse(session(), pkIndex, visitor);

        assertEquals("traversed indexes", Arrays.asList(expectedIndexes), visitor.records());
    }

    @Test @SuppressWarnings("unused") // invoked via JMX
    public void scanCustomers() throws InvalidOperationException {

        List<NewRow> actual = scanAll(scanAllRequest(customers));
        List<NewRow> expected = Arrays.asList(
                createNewRow(customers, 71L)
        );
        assertEquals("rows scanned", expected, actual);
    }

    @Test @SuppressWarnings("unused") // invoked via JMX
    public void scanOrders() throws InvalidOperationException {

        List<NewRow> actual = scanAll(scanAllRequest(orders));
        List<NewRow> expected = Arrays.asList(
                createNewRow(orders, 71L, 81L),
                createNewRow(orders, 72L, 82L)
        );
        assertEquals("rows scanned", expected, actual);
    }

    @Test @SuppressWarnings("unused") // invoked via JMX
    public void scanItems() throws InvalidOperationException {
        List<NewRow> actual = scanAll(scanAllRequest(items));
        List<NewRow> expected = Arrays.asList(
                createNewRow(items, 71L, 81L, 91L),
                createNewRow(items, 71L, 81L, 92L),
                createNewRow(items, 72L, 82L, 93L)
        );
        assertEquals("rows scanned", expected, actual);
    }
}
