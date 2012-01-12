/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.pt;

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public final class TraversalsPerWritePT extends PTBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        pb.add("1-LEFT", 1, Index.JoinType.LEFT);
        pb.add("20-LEFT", 20, Index.JoinType.LEFT);
        pb.add("1-RIGHT", 1, Index.JoinType.RIGHT);
        pb.add("20-RIGHT", 20, Index.JoinType.RIGHT);
        pb.multiplyParametersByAppending("-narrow", false, "-bushy", true);
        return pb.asList();
    }

    @Override
    protected void relevantTaps(TapsRegexes tapsRegexes) {
        tapsRegexes.add("travers.*");
    }

    @Test
    public void deleteCustomer1() {
        deleteRow(cTable, 1L, "alpha");
    }

    @Test
    public void writeCustomer0() {
        writeRow(cTable, 0L, "adopter");
    }

    @Test
    public void moveCustomer1To2() {
        update(cTable, 1L, "alpha").to(2L, "beta");
    }

    @Test
    public void moveCustomer1To0() {
        update(cTable, 1L, "alpha").to(0L, "adopter");
    }

    @Test
    public void writeOrderForCustomer0() {
        writeRows(ordersRow(0, ordersPerCustomer+1));
    }

    @Test
    public void writeOrderForCustomer1() {
        writeRows(ordersRow(1, ordersPerCustomer+1));
    }

    @Test
    public void writeOrderForCustomer9() {
        writeRows(ordersRow(9, ordersPerCustomer+1));
    }

    @Test
    public void deleteOrderForCustomer0() {
        dml().deleteRow(session(), ordersRow(0, ordersPerCustomer));
    }

    @Test
    public void deleteOrderForCustomer1() {
        dml().deleteRow(session(), ordersRow(1, ordersPerCustomer));
    }

    @Test
    public void moveOrderForCustomer0ToCustomer0() {
        update(ordersRow(0, ordersPerCustomer)).to(ordersRow(0, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer0ToCustomer1() {
        update(ordersRow(0, ordersPerCustomer)).to(ordersRow(1, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer0ToCustomer9() {
        update(ordersRow(0, ordersPerCustomer)).to(ordersRow(9, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer1ToCustomer0() {
        update(ordersRow(1, ordersPerCustomer)).to(ordersRow(0, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer1ToCustomer1() {
        update(ordersRow(1, ordersPerCustomer)).to(ordersRow(1, ordersPerCustomer+10));
    }

    @Test
    public void moveOrderForCustomer1ToCustomer9() {
        update(ordersRow(1, ordersPerCustomer)).to(ordersRow(9, ordersPerCustomer+10));
    }

    @Override
    protected String paramName() {
        return String.format("%d-%s", ordersPerCustomer, joinType);
    }

    @Override
    protected void beforeProfiling() {
        cTable = createTable(SCHEMA, "customers", "cid int key, name varchar(32)");
        int aTable = createTable(SCHEMA, "addresses", "aid int key, cid int, where varchar(32)",
                akibanFK("cid", "customers", "cid")
        );
        // creating the oTable after aTable will give it a higher ordinal, sandwiching it in the bushy group
        oTable = createTable(SCHEMA, "orders", "oid int key, cid int, when varchar(32)",
                akibanFK("cid", "customers", "cid)"));
        int pTable = createTable(SCHEMA, "pets", "pid int key, cid int, where varchar(32)",
                akibanFK("cid", "customers", "cid")
        );
        int vTable = createTable(SCHEMA, "vehicles", "vid int key, cid int, model varchar(32)",
                akibanFK("cid", "customers", "cid")
        );
        
        // write one customer
        writeRow(cTable, 1L, "alpha");
        // write orders for two customers, one of which (cid=0) doesn't exist
        for (long cid = 0; cid < 2; ++cid) {
            for (long oidSegment = 1; oidSegment <= ordersPerCustomer; ++oidSegment) {
                NewRow row = ordersRow(cid, oidSegment);
                writeRows(row);
                if (bushy) {
                    writeRows(
                            customersChild(aTable,  cid, oidSegment),
                            customersChild(pTable,  cid, oidSegment),
                            customersChild(vTable,  cid, oidSegment)
                    );
                }
            }
        }
        // write a third customer
        writeRow(cTable, 9L, "joda"); // like "iota", the ninth letter. This joke is a bit "forced"

        createGroupIndex(
                getUserTable(cTable).getGroup().getName(),
                "test_gi",
                "customers.name,orders.when",
                joinType
        );
    }

    private NewRow ordersRow(long cid, long oidSegment) {
        return customersChild(oTable, cid, oidSegment);
    }
    
    private NewRow customersChild(int tableId, long cid, long oidSegment) {
        long oid = cid + oidSegment  * 10;
        return createNewRow(tableId, oid, cid, String.valueOf(1900+oid));
    }

    public TraversalsPerWritePT(int ordersPerCustomer, Index.JoinType joinType, boolean bushy) {
        this.ordersPerCustomer = ordersPerCustomer;
        this.joinType = joinType;
        this.bushy = bushy;
    }

    private final int ordersPerCustomer;
    private final Index.JoinType joinType;
    private final boolean bushy;

    private int oTable;
    private int cTable;

    private static final String SCHEMA = "tpwpt";
}
