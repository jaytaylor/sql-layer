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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Group;
import com.foundationdb.server.rowdata.RowDef;

public class Schema
{
    // For all KeyUpdate*IT
    static Integer vendorId;
    static RowDef vendorRD;
    static Integer customerId;
    static RowDef customerRD;
    static Integer orderId;
    static RowDef orderRD;
    static Integer itemId;
    static RowDef itemRD;
    static Group group;
    // For KeyUpdateIT and KeyUpdateCascadingKeysIT
    static Integer v_vid;
    static Integer v_vx;
    static Integer c_cid;
    static Integer c_vid;
    static Integer c_cx;
    static Integer o_oid;
    static Integer o_cid;
    static Integer o_vid;
    static Integer o_ox;
    static Integer o_priority;
    static Integer o_when;
    static Integer i_vid;
    static Integer i_cid;
    static Integer i_oid;
    static Integer i_iid;
    static Integer i_ix;
    // For MultiColumnKeyUpdateIT and MultiColumnKeyUpdateCascadingKeysIT
    static Integer v_vid1;
    static Integer v_vid2;
    static Integer c_vid1;
    static Integer c_vid2;
    static Integer c_cid1;
    static Integer c_cid2;
    static Integer o_vid1;
    static Integer o_vid2;
    static Integer o_cid1;
    static Integer o_cid2;
    static Integer o_oid1;
    static Integer o_oid2;
    static Integer i_vid1;
    static Integer i_vid2;
    static Integer i_cid1;
    static Integer i_cid2;
    static Integer i_oid1;
    static Integer i_oid2;
    static Integer i_iid1;
    static Integer i_iid2;
}
