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

package com.akiban.server.test.pt.qp;

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.qp.operator.API.*;

@Ignore
public class SimpleJoinPT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        customer = createTable(
            "schema", "customer",
            "cid int not null",
            "name varchar(20)",
            "primary key(cid)");
        order = createTable(
            "schema", "orders",
            "oid int not null",
            "cid int",
            "salesman varchar(20)",
            "primary key(oid)",
            "grouping foreign key (cid) references customer(cid)");
        item = createTable(
            "schema", "item",
            "iid int not null",
            "oid int",
            "primary key(iid)",
            "grouping foreign key (oid) references orders(oid)");
        address = createTable(
            "schema", "address",
            "aid int not null",
            "cid int",
            "address varchar(100)",
            "primary key(aid)",
            "grouping foreign key (cid) references customer(cid)");
        createIndex("schema", "customer", "idx_cname", "name");
        createIndex("schema", "orders", "idx_osalesman", "salesman");
        createIndex("schema", "address", "idx_aaddress", "address");
        schema = new Schema(ais());
        customerRowType = schema.userTableRowType(userTable(customer));
        orderRowType = schema.userTableRowType(userTable(order));
        itemRowType = schema.userTableRowType(userTable(item));
        addressRowType = schema.userTableRowType(userTable(address));
        customerNameIndexRowType = indexType(customer, "name");
        orderSalesmanIndexRowType = indexType(order, "salesman");
        itemOidIndexRowType = indexType(item, "oid");
        itemIidIndexRowType = indexType(item, "iid");
        customerCidIndexRowType = indexType(customer, "cid");
        addressAddressIndexRowType = indexType(address, "address");
        coi = group(customer);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB(int customers, int ordersPerCustomer, int itemsPerOrder)
    {
        long cid = 0;
        long oid = 0;
        long iid = 0;
        for (int c = 0; c < customers; c++) {
            dml().writeRow(session(), createNewRow(customer, cid, String.format("customer %s", cid)));
            for (int o = 0; o < ordersPerCustomer; o++) {
                dml().writeRow(session(), createNewRow(order, oid, cid, String.format("salesman %s", oid)));
                for (int i = 0; i < itemsPerOrder; i++) {
                    dml().writeRow(session(), createNewRow(item, iid, oid));
                    iid++;
                }
                oid++;
            }
            cid++;
        }
    }

    @Test
    public void profileGroupScan()
    {
        final int SCANS = 100000000;
        final int CUSTOMERS = 1000;
        final int ORDERS_PER_CUSTOMER = 5;
        final int ITEMS_PER_ORDER = 2;
        populateDB(CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER);
        Operator plan = 
            flatten_HKeyOrdered(
                ancestorLookup_Default(
                    indexScan_Default(itemIidIndexRowType),
                    coi,
                    itemIidIndexRowType,
                    Arrays.asList(itemRowType, orderRowType),
                    InputPreservationOption.DISCARD_INPUT),
                orderRowType,
                itemRowType,
                JoinType.INNER_JOIN);
        Tap.setEnabled(".*", true);
        for (int s = 0; s < SCANS; s++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            while (cursor.next() != null) {
            }
            cursor.close();
        }
        TapReport[] reports = Tap.getReport(".*");
        for (TapReport report : reports) {
            System.out.println(report);
        }
    }

    protected int customer;
    protected int order;
    protected int item;
    protected int address;
    protected UserTableRowType customerRowType;
    protected UserTableRowType orderRowType;
    protected UserTableRowType itemRowType;
    protected UserTableRowType addressRowType;
    protected IndexRowType customerCidIndexRowType;
    protected IndexRowType customerNameIndexRowType;
    protected IndexRowType orderSalesmanIndexRowType;
    protected IndexRowType itemOidIndexRowType;
    protected IndexRowType itemIidIndexRowType;
    protected IndexRowType addressAddressIndexRowType;
    protected Group coi;
}
