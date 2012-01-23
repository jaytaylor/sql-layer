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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

public class HKeyDependentTableTest
{
    @Test
    public void test() throws Exception
    {
        String[] ddl = {
            "use s; ",
            // root table
            "create table s.r(",
            "    rid int not null key",
            ") engine = akibandb;",
            // For the following tables, "c" means that the key is cascaded, and "n" means it isn't.
            // "cn" means that the parent is cascaded but that this table isn't.
            "create table s.c(",
            "    rid int not null, ",
            "    cid int not null, ",
            "    primary key(rid, cid), ",
            "   constraint `__akiban_cr` foreign key (rid) references r(rid)",
            ") engine = akibandb;",
            "create table s.cc(",
            "    rid int not null, ",
            "    cid int not null, ",
            "    ccid int not null, ",
            "    primary key(rid, cid, ccid), ",
            "   constraint `__akiban_ccc` foreign key (rid, cid) references c(rid, cid)",
            ") engine = akibandb;",
            "create table s.cn(",
            "    cnid int not null, ",
            "    rid int, ",
            "    cid int, ",
            "    primary key(cnid), ",
            "   constraint `__akiban_cnc` foreign key (rid, cid) references c(rid, cid)",
            ") engine = akibandb;",
            "create table s.n(",
            "    nid int not null, ",
            "    rid int, ",
            "    primary key(nid), ",
            "   constraint `__akiban_nr` foreign key (rid) references r(rid)",
            ") engine = akibandb;",
            "create table s.nc(",
            "    nid int not null, ",
            "    ncid int not null, ",
            "    primary key(nid, ncid), ",
            "   constraint `__akiban_ncn` foreign key (nid) references n(nid)",
            ") engine = akibandb;",
            "create table s.nn(",
            "    nnid int not null, ",
            "    nid int, ",
            "    primary key(nnid), ",
            "   constraint `__akiban_nnn` foreign key (nid) references n(nid)",
            ") engine = akibandb;",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        UserTable r = ais.getUserTable("s", "r");
        UserTable c = ais.getUserTable("s", "c");
        UserTable cc = ais.getUserTable("s", "cc");
        UserTable cn = ais.getUserTable("s", "cn");
        UserTable n = ais.getUserTable("s", "n");
        UserTable nc = ais.getUserTable("s", "nc");
        UserTable nn = ais.getUserTable("s", "nn");
        // Check hkeys
        checkHKey(r.hKey(),
                  r, r, "rid");
        checkHKey(c.hKey(),
                  r, c, "rid",
                  c, c, "cid");
        checkHKey(cc.hKey(),
                  r, cc, "rid",
                  c, cc, "cid",
                  cc, cc, "ccid");
        checkHKey(cn.hKey(),
                  r, cn, "rid",
                  c, cn, "cid",
                  cn, cn, "cnid");
        checkHKey(n.hKey(),
                  r, n, "rid",
                  n, n, "nid");
        checkHKey(nc.hKey(),
                  r, n, "rid",
                  n, nc, "nid",
                  nc, nc, "ncid");
        checkHKey(nn.hKey(),
                  r, n, "rid",
                  n, nn, "nid",
                  nn, nn, "nnid");
        checkTables(Collections.<UserTable>emptyList(), r.hKeyDependentTables());
        checkTables(Collections.<UserTable>emptyList(), c.hKeyDependentTables());
        checkTables(Arrays.asList(nc, nn), n.hKeyDependentTables());
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
