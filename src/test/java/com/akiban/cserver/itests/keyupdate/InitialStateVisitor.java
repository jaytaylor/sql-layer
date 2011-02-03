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

package com.akiban.cserver.itests.keyupdate;

import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.dml.scan.NewRow;

import static com.akiban.cserver.itests.keyupdate.Schema.*;
import static junit.framework.Assert.*;

// Assumptions:
// - COI schema
// - oid / 10 = cid
// - iid / 10 = oid
// - cx = 100 * cid
// - ox = 100 * oid
// - ix = 100 * iid

class InitialStateRecordVisistor extends KeyUpdateTestRecordVisistor
{
    @Override
    public void visit(Object[] key, NewRow row)
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
    }
}