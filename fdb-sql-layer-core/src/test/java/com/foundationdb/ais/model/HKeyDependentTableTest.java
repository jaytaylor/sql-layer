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

package com.foundationdb.ais.model;

import com.foundationdb.server.rowdata.SchemaFactory;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

public class HKeyDependentTableTest
{
    @Test
    public void test() throws Exception
    {
        // Naming convention:
        //
        // r: root
        // c: cascaded keys
        // n: normal (not cascaded) keys
        //
        // "cn" means that the parent is cascaded but that the child table isn't.
        String[] ddl = {
            // 1st level - root table
            "create table s.r(",
            "    rid int not null primary key",
            ");",
            // 2nd level
            "create table s.c(",
            "    rid int not null, ",
            "    cid int not null, ",
            "    primary key(rid, cid), ",
            "    grouping foreign key (rid) references r(rid)",
            ");",
            "create table s.n(",
            "    rid int, ",
            "    nid int not null, ",
            "    primary key(nid), ",
            "    grouping foreign key (rid) references r(rid)",
            ");",
            // 3rd level
            "create table s.cc(",
            "    rid int not null, ",
            "    cid int not null, ",
            "    ccid int not null, ",
            "    primary key(rid, cid, ccid), ",
            "    grouping foreign key (rid, cid) references c(rid, cid)",
            ");",
            "create table s.cn(",
            "    rid int, ",
            "    cid int, ",
            "    cnid int not null, ",
            "    primary key(cnid), ",
            "    grouping foreign key (rid, cid) references c(rid, cid)",
            ");",
            "create table s.nc(",
            "    nid int not null, ",
            "    ncid int not null, ",
            "    primary key(nid, ncid), ",
            "    grouping foreign key (nid) references n(nid)",
            ");",
            "create table s.nn(",
            "    nid int, ",
            "    nnid int not null, ",
            "    primary key(nnid), ",
            "    grouping foreign key (nid) references n(nid)",
            ");",
            // 4th level
            "create table s.ccc(",
            "    rid int not null, ",
            "    cid int not null, ",
            "    ccid int not null, ",
            "    cccid int not null, ",
            "    primary key(rid, cid, ccid, cccid), ",
            "    grouping foreign key (rid, cid, ccid) references cc(rid, cid, ccid)",
            ");",
            "create table s.ccn(",
            "    rid int, ",
            "    cid int, ",
            "    ccid int, ",
            "    ccnid int not null, ",
            "    primary key(ccnid), ",
            "    grouping foreign key (rid, cid, ccid) references cc(rid, cid, ccid)",
            ");",
            "create table s.cnc(",
            "    cnid int not null, ",
            "    cncid int not null, ",
            "    primary key(cnid, cncid), ",
            "    grouping foreign key (cnid) references cn(cnid)",
            ");",
            "create table s.cnn(",
            "    cnid int, ",
            "    cnnid int not null, ",
            "    primary key(cnnid), ",
            "    grouping foreign key (cnid) references cn(cnid)",
            ");",
            "create table s.ncc(",
            "    nid int, ",
            "    ncid int not null, ",
            "    nccid int not null, ",
            "    primary key(ncid, nccid), ",
            "    grouping foreign key (nid, ncid) references nc(nid, ncid)",
            ");",
            "create table s.ncn(",
            "    nid int, ",
            "    ncid int, ",
            "    ncnid int not null, ",
            "    primary key(ncnid), ",
            "    grouping foreign key (nid, ncid) references nc(nid, ncid)",
            ");",
            "create table s.nnc(",
            "    nnid int not null, ",
            "    nncid int not null, ",
            "    primary key(nnid, nncid), ",
            "    grouping foreign key (nnid) references nn(nnid)",
            ");",
            "create table s.nnn(",
            "    nnid int, ",
            "    nnnid int not null, ",
            "    primary key(nnnid), ",
            "    grouping foreign key (nnid) references nn(nnid)",
            ");",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        Table r = ais.getTable("s", "r");
        Table c = ais.getTable("s", "c");
        Table n = ais.getTable("s", "n");
        Table cc = ais.getTable("s", "cc");
        Table cn = ais.getTable("s", "cn");
        Table nc = ais.getTable("s", "nc");
        Table nn = ais.getTable("s", "nn");
        Table ccc = ais.getTable("s", "ccc");
        Table ccn = ais.getTable("s", "ccn");
        Table cnc = ais.getTable("s", "cnc");
        Table cnn = ais.getTable("s", "cnn");
        Table ncc = ais.getTable("s", "ncc");
        Table ncn = ais.getTable("s", "ncn");
        Table nnc = ais.getTable("s", "nnc");
        Table nnn = ais.getTable("s", "nnn");
        // Check hkeys
        checkHKey(r.hKey(),
                  r, r, "rid");
        checkHKey(c.hKey(),
                  r, c, "rid",
                  c, c, "cid");
        checkHKey(n.hKey(),
                  r, n, "rid",
                  n, n, "nid");
        checkHKey(cc.hKey(),
                  r, cc, "rid",
                  c, cc, "cid",
                  cc, cc, "ccid");
        checkHKey(cn.hKey(),
                  r, cn, "rid",
                  c, cn, "cid",
                  cn, cn, "cnid");
        checkHKey(nc.hKey(),
                  r, n, "rid",
                  n, nc, "nid",
                  nc, nc, "ncid");
        checkHKey(nn.hKey(),
                  r, n, "rid",
                  n, nn, "nid",
                  nn, nn, "nnid");
        checkHKey(ccc.hKey(),
                  r, ccc, "rid",
                  c, ccc, "cid",
                  cc, ccc, "ccid",
                  ccc, ccc, "cccid");
        checkHKey(ccn.hKey(),
                  r, ccn, "rid",
                  c, ccn, "cid",
                  cc, ccn, "ccid",
                  ccn, ccn, "ccnid");
        checkHKey(cnc.hKey(),
                  r, cn, "rid",
                  c, cn, "cid",
                  cn, cnc, "cnid",
                  cnc, cnc, "cncid");
        checkHKey(cnn.hKey(),
                  r, cn, "rid",
                  c, cn, "cid",
                  cn, cnn, "cnid",
                  cnn, cnn, "cnnid");
        checkHKey(ncc.hKey(),
                  r, n, "rid",
                  n, ncc, "nid",
                  nc, ncc, "ncid",
                  ncc, ncc, "nccid");
        checkHKey(ncn.hKey(),
                  r, n, "rid",
                  n, ncn, "nid",
                  nc, ncn, "ncid",
                  ncn, ncn, "ncnid");
        checkHKey(nnc.hKey(),
                  r, n, "rid",
                  n, nn, "nid",
                  nn, nnc, "nnid",
                  nnc, nnc, "nncid");
        checkHKey(nnn.hKey(),
                  r, n, "rid",
                  n, nn, "nid",
                  nn, nnn, "nnid",
                  nnn, nnn, "nnnid");
        checkTables(NO_DEPENDENTS, r.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, c.hKeyDependentTables());
        checkTables(tables(nc, nn, ncc, ncn, nnc, nnn), n.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, cc.hKeyDependentTables());
        checkTables(tables(cnc, cnn), cn.hKeyDependentTables());
        checkTables(tables(ncc, ncn), nc.hKeyDependentTables());
        checkTables(tables(nnc, nnn), nn.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, ccc.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, ccn.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, cnc.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, cnn.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, ncc.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, ncn.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, nnc.hKeyDependentTables());
        checkTables(NO_DEPENDENTS, nnn.hKeyDependentTables());
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
    
    private void checkTables(List<Table> expected, List<Table> actual)
    {
        // Check contents, not order
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }
    
    private List<Table> tables(Table ... tables)
    {
        return Arrays.asList(tables);
    }

    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory("s");
    private static final List<Table> NO_DEPENDENTS = Collections.emptyList();
}
