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

package com.akiban.server.itests.hapi.randomdb;

enum Column
{
    // Customer
    C_CID("cid"),
    C_CID_COPY("cid_copy"),
    // Order
    O_CID("cid"),
    O_OID("oid"),
    O_CID_COPY("cid_copy"),
    // Item
    I_CID("cid"),
    I_OID("oid"),
    I_IID("iid"),
    I_CID_COPY("cid_copy"),
    // Address
    A_CID("cid"),
    A_AID("aid"),
    A_CID_COPY("cid_copy");

    public String columnName()
    {
        return name;
    }

    Column(String name)
    {
        this.name = name;
    }

    private String name;
}
