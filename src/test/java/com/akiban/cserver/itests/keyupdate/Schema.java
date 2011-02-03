package com.akiban.cserver.itests.keyupdate;

import com.akiban.cserver.RowDef;

/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

public class Schema
{
    static int customerId;
    static int c_cid;
    static int c_cx;
    static RowDef customerRowDef;
    static int orderId;
    static int o_oid;
    static int o_cid;
    static int o_ox;
    static RowDef orderRowDef;
    static int itemId;
    static int i_iid;
    static int i_oid;
    static int i_ix;
    static RowDef itemRowDef;
    static RowDef groupRowDef;
}
