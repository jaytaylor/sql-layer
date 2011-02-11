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
    static Integer customerId;
    static Integer c_cid;
    static Integer c_cx;
    static RowDef customerRowDef;
    static Integer orderId;
    static Integer o_oid;
    static Integer o_cid;
    static Integer o_ox;
    static RowDef orderRowDef;
    static Integer itemId;
    static Integer i_cid;
    static Integer i_oid;
    static Integer i_iid;
    static Integer i_ix;
    static RowDef itemRowDef;
    static RowDef groupRowDef;
}
