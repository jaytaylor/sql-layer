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

package com.akiban.server.test.it.hapi.randomdb;

import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.ApiTestBase;

class Database
{
    public Database(RCTortureIT test)
    {
        this.test = test;
    }

    public void createSchema() throws InvalidOperationException
    {
        test.customerTable = test.table(
            RCTortureIT.SCHEMA, "customer",
            "cid int not null",
            "cid_copy int not null",
            "primary key(cid)");
        test.index(RCTortureIT.SCHEMA, "customer", "idx_cid_copy", "cid_copy");
        test.orderTable = test.table(
            RCTortureIT.SCHEMA, "order",
            "cid int not null",
            "oid int not null",
            "cid_copy int not null",
            "primary key(cid, oid)",
            "grouping foreign key (cid) references customer(cid)");
        test.index(RCTortureIT.SCHEMA, "order", "idx_cid_copy", "cid_copy");
        test.itemTable = test.table(
            RCTortureIT.SCHEMA, "item",
            "cid int not null",
            "oid int not null",
            "iid int not null",
            "cid_copy int not null",
            "primary key(cid, oid, iid)",
            "grouping foreign key (cid, oid) references \"order\"(cid, oid)");
        test.index(RCTortureIT.SCHEMA, "item", "idx_cid_copy", "cid_copy");
        test.addressTable = test.table(
            RCTortureIT.SCHEMA, "address",
            "cid int not null",
            "aid int not null",
            "cid_copy int not null",
            "primary key(cid, aid)",
            "grouping foreign key (cid) references customer(cid)");
        test.index(RCTortureIT.SCHEMA, "address", "idx_cid_copy", "cid_copy");
    }

    public void populate() throws Exception
    {
        test.db.clear();
        Sampler sampler;
        int nCustomer = test.random.nextInt(RCTortureIT.MAX_CUSTOMERS + 1);
        sampler = new Sampler(test.random, nCustomer);
        for (int i = 0; i < nCustomer; i++) {
            long cid = sampler.take();
            NewRow row = ApiTestBase.createNewRow(test.getStore(), test.customerTable, cid, cid);
            test.addRow(row);
        }
        int nOrder = test.random.nextInt(RCTortureIT.MAX_CUSTOMERS * RCTortureIT.MAX_ORDERS_PER_CUSTOMER + 1);
        sampler = new Sampler(test.random, nOrder);
        for (int i = 0; i < nOrder; i++) {
            long cidOid = sampler.take();
            long cid = cidOid / RCTortureIT.MAX_ORDERS_PER_CUSTOMER;
            long oid = cidOid % RCTortureIT.MAX_ORDERS_PER_CUSTOMER;
            NewRow row = ApiTestBase.createNewRow(test.getStore(), test.orderTable, cid, oid, cid);
            test.addRow(row);
        }
        int nItem = test.random.nextInt(RCTortureIT.MAX_CUSTOMERS * RCTortureIT.MAX_ORDERS_PER_CUSTOMER * RCTortureIT.MAX_ITEMS_PER_ORDER + 1);
        sampler = new Sampler(test.random, nItem);
        for (int i = 0; i < nItem; i++) {
            long cidOidIid = sampler.take();
            long cid = cidOidIid / (RCTortureIT.MAX_ORDERS_PER_CUSTOMER * RCTortureIT.MAX_ITEMS_PER_ORDER);
            long oid = (cidOidIid / RCTortureIT.MAX_ITEMS_PER_ORDER) % RCTortureIT.MAX_ORDERS_PER_CUSTOMER;
            long iid = cidOidIid % RCTortureIT.MAX_ITEMS_PER_ORDER;
            NewRow row = ApiTestBase.createNewRow(test.getStore(), test.itemTable, cid, oid, iid, cid);
            test.addRow(row);
        }
        int nAddress = test.random.nextInt(RCTortureIT.MAX_CUSTOMERS * RCTortureIT.MAX_ADDRESSES_PER_CUSTOMER + 1);
        sampler = new Sampler(test.random, nAddress);
        for (int i = 0; i < nAddress; i++) {
            long cidAid = sampler.take();
            long cid = cidAid / RCTortureIT.MAX_ADDRESSES_PER_CUSTOMER;
            long aid = cidAid % RCTortureIT.MAX_ADDRESSES_PER_CUSTOMER;
            NewRow row = ApiTestBase.createNewRow(test.getStore(), test.addressTable, cid, aid, cid);
            test.addRow(row);
        }
        test.sort(test.db);
        test.printDB();
    }

    private RCTortureIT test;
}
