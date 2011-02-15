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

package com.akiban.server.itests.keyupdate;

import com.akiban.server.RowDef;

public class Schema
{
    // For all KeyUpdate*IT
    static Integer customerId;
    static RowDef customerRowDef;
    static Integer orderId;
    static RowDef orderRowDef;
    static Integer itemId;
    static RowDef itemRowDef;
    static RowDef groupRowDef;
    // For KeyUpdateIT and KeyUpdateCascadingKeysIT
    static Integer c_cid;
    static Integer c_cx;
    static Integer o_oid;
    static Integer o_cid;
    static Integer o_ox;
    static Integer i_cid;
    static Integer i_oid;
    static Integer i_iid;
    static Integer i_ix;
    // For KeyUpdateWithMoreComplexSchemaIT
    static Integer c_cid1;
    static Integer c_cid2;
    static Integer c_s1;
    static Integer c_s2;
    static Integer c_s3;
    static Integer c_s4;
    static Integer c_s5;
    static Integer c_s6;
    static Integer o_oid1;
    static Integer o_oid2;
    static Integer o_cid1;
    static Integer o_cid2;
    static Integer o_s1;
    static Integer o_s2;
    static Integer o_s3;
    static Integer o_s4;
    static Integer o_s5;
    static Integer o_s6;
    static Integer o_s7;
    static Integer o_s8;
    static Integer i_iid1;
    static Integer i_iid2;
    static Integer i_oid1;
    static Integer i_oid2;
    static Integer i_s1;
    static Integer i_s2;
    static Integer i_s3;
    static Integer i_s4;
    static Integer i_s5;
}