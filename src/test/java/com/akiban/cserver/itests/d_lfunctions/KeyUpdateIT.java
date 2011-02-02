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

package com.akiban.cserver.itests.d_lfunctions;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.itests.ApiTestBase;
import com.akiban.cserver.store.TreeRecordVisitor;
import com.persistit.exception.PersistitException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.*;

public class KeyUpdateIT extends ApiTestBase
{
    @Before
    public void before() throws Exception
    {
        createSchema();
        populateTables();
    }

    @Test
    public void testInitialState() throws InvalidOperationException, PersistitException
    {
        int groupRowDefId = customerRowDef.getGroupRowDefId();
        RowDef groupRowDef = store().getRowDefCache().getRowDef(groupRowDefId);
        InitialStateVisistor initialStateVisistor = new InitialStateVisistor();
        store().traverse(session, groupRowDef, initialStateVisistor);
        checkHKeys(initialStateVisistor.keys(),
                   new Object[]{customerRowDef, 1L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 11L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 11L, itemRowDef, 111L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 11L, itemRowDef, 112L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 11L, itemRowDef, 113L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 12L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 12L, itemRowDef, 121L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 12L, itemRowDef, 122L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 12L, itemRowDef, 123L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 13L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 13L, itemRowDef, 131L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 13L, itemRowDef, 132L},
                   new Object[]{customerRowDef, 1L, orderRowDef, 13L, itemRowDef, 133L},
                   new Object[]{customerRowDef, 2L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 21L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 21L, itemRowDef, 211L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 21L, itemRowDef, 212L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 21L, itemRowDef, 213L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 22L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 221L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 223L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 23L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 23L, itemRowDef, 231L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 23L, itemRowDef, 232L},
                   new Object[]{customerRowDef, 2L, orderRowDef, 23L, itemRowDef, 233L},
                   new Object[]{customerRowDef, 3L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 31L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 31L, itemRowDef, 311L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 31L, itemRowDef, 312L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 31L, itemRowDef, 313L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 32L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 32L, itemRowDef, 321L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 32L, itemRowDef, 322L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 32L, itemRowDef, 323L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 33L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 33L, itemRowDef, 331L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 33L, itemRowDef, 332L},
                   new Object[]{customerRowDef, 3L, orderRowDef, 33L, itemRowDef, 333L});
    }

    private void createSchema() throws InvalidOperationException
    {
        customerId = createTable("coi", "customer",
                                 "cid int not null key",
                                 "cx int");
        c_cid = 0;
        c_cx = 1;
        orderId = createTable("coi", "order",
                              "oid int not null key",
                              "cid int",
                              "ox int",
                              "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)");
        o_oid = 0;
        o_cid = 1;
        o_ox = 2;
        itemId = createTable("coi", "item",
                             "iid int not null key",
                             "oid int",
                             "ix int",
                             "constraint __akiban_io foreign key __akiban_io(oid) references order(oid)");
        i_iid = 0;
        i_oid = 1;
        i_ix = 2;
        orderRowDef = rowDefCache().getRowDef(orderId);
        customerRowDef = rowDefCache().getRowDef(customerId);
        itemRowDef = rowDefCache().getRowDef(itemId);
    }

    private void checkHKeys(List<List<Object>> actual, Object[]... expected)
    {
        assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> a = actual.get(i);
            Object[] e = expected[i];
            assertEquals(e.length, a.size());
            for (int j = 0; j < e.length; j++) {
                if (e[j] instanceof RowDef) {
                    assertSame(((RowDef)e[j]).userTable(), a.get(j));
                } else {
                    assertEquals(e[j], a.get(j));
                }
            }
        }
    }

    private void checkHKey(List<Object> actualHKey, Object... expectedHKey)
    {
        assertEquals(expectedHKey.length, actualHKey.size());
        for (int i = 0; i < expectedHKey.length; i++) {
            Object actual = actualHKey.get(i);
            Object expected = expectedHKey[i];
            if (expected instanceof RowDef) {
                assertSame(((RowDef) expected).userTable(), actual);
            }
        }
    }

    private void populateTables() throws Exception
    {
        insert(row(customerRowDef, 1, 100));
        insert(row(customerRowDef, 2, 200));
        insert(row(customerRowDef, 3, 300));
        insert(row(orderRowDef, 11, 1, 1100));
        insert(row(orderRowDef, 12, 1, 1200));
        insert(row(orderRowDef, 13, 1, 1300));
        insert(row(orderRowDef, 21, 2, 2100));
        insert(row(orderRowDef, 22, 2, 2200));
        insert(row(orderRowDef, 23, 2, 2300));
        insert(row(orderRowDef, 31, 3, 3100));
        insert(row(orderRowDef, 32, 3, 3200));
        insert(row(orderRowDef, 33, 3, 3300));
        insert(row(itemRowDef, 111, 11, 11100));
        insert(row(itemRowDef, 112, 11, 11200));
        insert(row(itemRowDef, 113, 11, 11300));
        insert(row(itemRowDef, 121, 12, 12100));
        insert(row(itemRowDef, 122, 12, 12200));
        insert(row(itemRowDef, 123, 12, 12300));
        insert(row(itemRowDef, 131, 13, 13100));
        insert(row(itemRowDef, 132, 13, 13200));
        insert(row(itemRowDef, 133, 13, 13300));
        insert(row(itemRowDef, 211, 21, 21100));
        insert(row(itemRowDef, 212, 21, 21200));
        insert(row(itemRowDef, 213, 21, 21300));
        insert(row(itemRowDef, 221, 22, 22100));
        insert(row(itemRowDef, 222, 22, 22200));
        insert(row(itemRowDef, 223, 22, 22300));
        insert(row(itemRowDef, 231, 23, 23100));
        insert(row(itemRowDef, 232, 23, 23200));
        insert(row(itemRowDef, 233, 23, 23300));
        insert(row(itemRowDef, 311, 31, 31100));
        insert(row(itemRowDef, 312, 31, 31200));
        insert(row(itemRowDef, 313, 31, 31300));
        insert(row(itemRowDef, 321, 32, 32100));
        insert(row(itemRowDef, 322, 32, 32200));
        insert(row(itemRowDef, 323, 32, 32300));
        insert(row(itemRowDef, 331, 33, 33100));
        insert(row(itemRowDef, 332, 33, 33200));
        insert(row(itemRowDef, 333, 33, 33300));
    }

    private NiceRow row(RowDef table, Object... values)
    {
        int tableId = table.getRowDefId();
        NiceRow row = new NiceRow(tableId, table);
        int column = 0;
        for (Object value : values) {
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            }
            row.put(column++, value);
        }
        return row;
    }

    private void insert(NiceRow row) throws Exception
    {
        dml().writeRow(session, row);
    }

    private int customerId;
    private int c_cid;
    private int c_cx;
    private RowDef customerRowDef;
    private int orderId;
    private int o_oid;
    private int o_cid;
    private int o_ox;
    private RowDef orderRowDef;
    private int itemId;
    private int i_iid;
    private int i_oid;
    private int i_ix;
    private RowDef itemRowDef;

    private class InitialStateVisistor extends TreeRecordVisitor
    {
        @Override
        public void visit(List<Object> key, NewRow row)
        {
            RowDef rowDef = row.getRowDef();
            if (rowDef == customerRowDef) {
                Long cid = (Long) row.get(c_cid);
                Long cx = (Long) row.get(c_cx);
                assertEquals(cid * 100, cx.longValue());
                checkHKey(key, customerRowDef, cid);
            } else if (rowDef == orderRowDef) {
                Long oid = (Long) row.get(o_oid);
                Long cid = (Long) row.get(o_cid);
                Long ox = (Long) row.get(o_ox);
                assertEquals(oid * 100, ox.longValue());
                assertEquals(oid / 10, cid.longValue());
                checkHKey(key, customerRowDef, cid, orderRowDef, oid);
            } else if (rowDef == itemRowDef) {
                Long iid = (Long) row.get(i_iid);
                Long oid = (Long) row.get(i_oid);
                Long ix = (Long) row.get(i_ix);
                assertEquals(iid * 100, ix.longValue());
                assertEquals(iid / 10, oid.longValue());
                checkHKey(key, customerRowDef, iid / 100, orderRowDef, oid, itemRowDef, iid);
            } else {
                assertTrue(false);
            }
            keys.add(key);
        }

        public List<List<Object>> keys()
        {
            return keys;
        }

        private final List<List<Object>> keys = new ArrayList<List<Object>>();
    }
}
