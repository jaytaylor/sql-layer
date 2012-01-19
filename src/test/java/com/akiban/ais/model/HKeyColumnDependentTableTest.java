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

package com.akiban.ais.model;

import com.akiban.server.rowdata.SchemaFactory;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class HKeyColumnDependentTableTest
{
    @Test
    public void testNonCascadeSchema() throws Exception
    {
        String[] ddl = {
            "use schema; ",
            "create table schema.customer(",
            "    cid1 int not null, ",
            "    cid2 int not null, ",
            "    primary key(cid1, cid2) ",
            ") engine = akibandb;",
            "create table schema.order(",
            "    oid1 int not null, ",
            "    oid2 int not null, ",
            "    cid1 int, ",
            "    cid2 int, ",
            "    primary key(oid1, oid2), ",
            "   constraint `__akiban_oc` foreign key (cid1, cid2) references customer(cid1, cid2)",
            ") engine = akibandb;",
            "create table schema.item(",
            "    iid1 int not null, ",
            "    iid2 int not null, ",
            "    oid1 int, ",
            "    oid2 int, ",
            "    primary key(iid1, iid2), ",
            "   constraint `__akiban_io` foreign key (oid1, oid2) references order(oid1, oid2)",
            ") engine = akibandb;",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        UserTable customer = ais.getUserTable("schema", "customer");
        UserTable order = ais.getUserTable("schema", "order");
        UserTable item = ais.getUserTable("schema", "item");
        // Check hkeys
        checkHKey(customer.hKey(),
                  customer, customer, "cid1", customer, "cid2");
        checkHKey(order.hKey(),
                  customer, order, "cid1", order, "cid2",
                  order, order, "oid1", order, "oid2");
        checkHKey(item.hKey(),
                  customer, order, "cid1", order, "cid2",
                  order, item, "oid1", item, "oid2",
                  item, item, "iid1", item, "iid2");
        // Check customer hkey columns
        for (HKeyColumn hKeyColumn : hKeyColumns(customer)) {
            checkTables(Arrays.asList(customer), hKeyColumn.dependentTables());
        }
        // Check order hkey columns
        for (HKeyColumn hKeyColumn : hKeyColumns(order)) {
            if (hKeyColumn.column().getName().startsWith("cid")) {
                checkTables(Arrays.asList(order, item), hKeyColumn.dependentTables());
            } else if (hKeyColumn.column().getName().startsWith("oid")) {
                checkTables(Arrays.asList(order), hKeyColumn.dependentTables());
            }
        }
        // Check item hkey columns
        for (HKeyColumn hKeyColumn : hKeyColumns(item)) {
            if (hKeyColumn.column().getName().startsWith("cid")) {
                assertNull(hKeyColumn.dependentTables());
            } else if (hKeyColumn.column().getName().startsWith("oid")) {
                checkTables(Arrays.asList(item), hKeyColumn.dependentTables());
            } else if (hKeyColumn.column().getName().startsWith("iid")) {
                checkTables(Arrays.asList(item), hKeyColumn.dependentTables());
            }
        }
    }
    
    @Test
    public void testCascadeSchema() throws Exception
    {
        String[] ddl = {
            "use schema; ",
            "create table schema.customer(",
            "    cid1 int not null, ",
            "    cid2 int not null, ",
            "    primary key(cid1, cid2) ",
            ") engine = akibandb;",
            "create table schema.order(",
            "    cid1 int not null, ",
            "    cid2 int not null, ",
            "    oid1 int not null, ",
            "    oid2 int not null, ",
            "    primary key(cid1, cid2, oid1, oid2), ",
            "   constraint `__akiban_oc` foreign key (cid1, cid2) references customer(cid1, cid2)",
            ") engine = akibandb;",
            "create table schema.item(",
            "    cid1 int not null, ",
            "    cid2 int not null, ",
            "    oid1 int not null, ",
            "    oid2 int not null, ",
            "    iid1 int not null, ",
            "    iid2 int not null, ",
            "    primary key(cid1, cid2, oid2, oid2, iid1, iid2), ",
            "   constraint `__akiban_io` foreign key (cid1, cid2, oid1, oid2) references order(cid1, cid2, oid1, oid2)",
            ") engine = akibandb;",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        UserTable customer = ais.getUserTable("schema", "customer");
        UserTable order = ais.getUserTable("schema", "order");
        UserTable item = ais.getUserTable("schema", "item");
        // Check hkeys
        checkHKey(customer.hKey(),
                  customer, customer, "cid1", customer, "cid2");
        checkHKey(order.hKey(),
                  customer, order, "cid1", order, "cid2",
                  order, order, "oid1", order, "oid2");
        checkHKey(item.hKey(),
                  customer, item, "cid1", item, "cid2",
                  order, item, "oid1", item, "oid2",
                  item, item, "iid1", item, "iid2");
        // Check customer hkey columns
        for (HKeyColumn hKeyColumn : hKeyColumns(customer)) {
            checkTables(Arrays.asList(customer), hKeyColumn.dependentTables());
        }
        // Check order hkey columns
        for (HKeyColumn hKeyColumn : hKeyColumns(order)) {
            checkTables(Arrays.asList(order), hKeyColumn.dependentTables());
        }
        // Check item hkey columns
        for (HKeyColumn hKeyColumn : hKeyColumns(item)) {
            checkTables(Arrays.asList(item), hKeyColumn.dependentTables());
        }
    }

    private List<HKeyColumn> hKeyColumns(UserTable table)
    {
        List<HKeyColumn> hKeyColumns = new ArrayList<HKeyColumn>();
        for (HKeySegment segment : table.hKey().segments()) {
            for (HKeyColumn column : segment.columns()) {
                hKeyColumns.add(column);
            }
        }
        return hKeyColumns;
    }

    private void checkHKey(HKey hKey, Object ... elements)
    {
        int e = 0;
        int position = 0;
        for (HKeySegment segment : hKey.segments()) {
            assertEquals(position++, segment.positionInHKey());
            assertSame(elements[e++], segment.table());
            for (HKeyColumn column : segment.columns()) {
                assertEquals(position++, column.positionInHKey());
                assertEquals(elements[e++], column.column().getTable());
                assertEquals(elements[e++], column.column().getName());
            }
        }
        assertEquals(elements.length, e);
    }
    
    private void checkTables(List<UserTable> expected, List<UserTable> actual)
    {
        // Check contents, not order
        assertEquals(new HashSet<UserTable>(expected), new HashSet<UserTable>(actual));
    }

    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory();
}
