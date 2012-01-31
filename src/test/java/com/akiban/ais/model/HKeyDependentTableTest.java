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
        // Naming convention:
        //
        // r: root
        // c: cascaded keys
        // n: normal (not cascaded) keys
        //
        // "cn" means that the parent is cascaded but that the child table isn't.
        String[] ddl = {
            "use s; ",
            // 1st level - root table
            "create table s.r(",
            "    rid int not null key",
            ") engine = akibandb;",
            // 2nd level
            "create table s.c(",
            "    rid int not null, ",
            "    cid int not null, ",
            "    primary key(rid, cid), ",
            "   constraint `__akiban_c_r` foreign key (rid) references r(rid)",
            ") engine = akibandb;",
            "create table s.n(",
            "    rid int, ",
            "    nid int not null, ",
            "    primary key(nid), ",
            "   constraint `__akiban_n_r` foreign key (rid) references r(rid)",
            ") engine = akibandb;",
            // 3rd level
            "create table s.cc(",
            "    rid int not null, ",
            "    cid int not null, ",
            "    ccid int not null, ",
            "    primary key(rid, cid, ccid), ",
            "   constraint `__akiban_cc_c` foreign key (rid, cid) references c(rid, cid)",
            ") engine = akibandb;",
            "create table s.cn(",
            "    rid int, ",
            "    cid int, ",
            "    cnid int not null, ",
            "    primary key(cnid), ",
            "   constraint `__akiban_cn_c` foreign key (rid, cid) references c(rid, cid)",
            ") engine = akibandb;",
            "create table s.nc(",
            "    nid int not null, ",
            "    ncid int not null, ",
            "    primary key(nid, ncid), ",
            "   constraint `__akiban_nc_n` foreign key (nid) references n(nid)",
            ") engine = akibandb;",
            "create table s.nn(",
            "    nid int, ",
            "    nnid int not null, ",
            "    primary key(nnid), ",
            "   constraint `__akiban_nn_n` foreign key (nid) references n(nid)",
            ") engine = akibandb;",
            // 4th level
            "create table s.ccc(",
            "    rid int not null, ",
            "    cid int not null, ",
            "    ccid int not null, ",
            "    cccid int not null, ",
            "    primary key(rid, cid, ccid, cccid), ",
            "   constraint `__akiban_ccc_cc` foreign key (rid, cid, ccid) references cc(rid, cid, ccid)",
            ") engine = akibandb;",
            "create table s.ccn(",
            "    rid int, ",
            "    cid int, ",
            "    ccid int, ",
            "    ccnid int not null, ",
            "    primary key(ccnid), ",
            "   constraint `__akiban_ccn_cc` foreign key (rid, cid, ccid) references cc(rid, cid, ccid)",
            ") engine = akibandb;",
            "create table s.cnc(",
            "    cnid int not null, ",
            "    cncid int not null, ",
            "    primary key(cnid, cncid), ",
            "   constraint `__akiban_cnc_cn` foreign key (cnid) references cn(cnid)",
            ") engine = akibandb;",
            "create table s.cnn(",
            "    cnid int, ",
            "    cnnid int not null, ",
            "    primary key(cnnid), ",
            "   constraint `__akiban_cnn_cn` foreign key (cnid) references cn(cnid)",
            ") engine = akibandb;",
            "create table s.ncc(",
            "    nid int, ",
            "    ncid int not null, ",
            "    nccid int not null, ",
            "    primary key(ncid, nccid), ",
            "   constraint `__akiban_ncc_nc` foreign key (nid, ncid) references nc(nid, ncid)",
            ") engine = akibandb;",
            "create table s.ncn(",
            "    nid int, ",
            "    ncid int, ",
            "    ncnid int not null, ",
            "    primary key(ncnid), ",
            "   constraint `__akiban_ncn_nc` foreign key (nid, ncid) references nc(nid, ncid)",
            ") engine = akibandb;",
            "create table s.nnc(",
            "    nnid int not null, ",
            "    nncid int not null, ",
            "    primary key(nnid, nncid), ",
            "   constraint `__akiban_nnc_nn` foreign key (nnid) references nn(nnid)",
            ") engine = akibandb;",
            "create table s.nnn(",
            "    nnid int, ",
            "    nnnid int not null, ",
            "    primary key(nnnid), ",
            "   constraint `__akiban_nnn_nn` foreign key (nnid) references nn(nnid)",
            ") engine = akibandb;",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        UserTable r = ais.getUserTable("s", "r");
        UserTable c = ais.getUserTable("s", "c");
        UserTable n = ais.getUserTable("s", "n");
        UserTable cc = ais.getUserTable("s", "cc");
        UserTable cn = ais.getUserTable("s", "cn");
        UserTable nc = ais.getUserTable("s", "nc");
        UserTable nn = ais.getUserTable("s", "nn");
        UserTable ccc = ais.getUserTable("s", "ccc");
        UserTable ccn = ais.getUserTable("s", "ccn");
        UserTable cnc = ais.getUserTable("s", "cnc");
        UserTable cnn = ais.getUserTable("s", "cnn");
        UserTable ncc = ais.getUserTable("s", "ncc");
        UserTable ncn = ais.getUserTable("s", "ncn");
        UserTable nnc = ais.getUserTable("s", "nnc");
        UserTable nnn = ais.getUserTable("s", "nnn");
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
    
    private void checkTables(List<UserTable> expected, List<UserTable> actual)
    {
        // Check contents, not order
        assertEquals(new HashSet<UserTable>(expected), new HashSet<UserTable>(actual));
    }
    
    private List<UserTable> tables(UserTable ... tables)
    {
        return Arrays.asList(tables);
    }

    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory();
    private static final List<UserTable> NO_DEPENDENTS = Collections.emptyList();
}
