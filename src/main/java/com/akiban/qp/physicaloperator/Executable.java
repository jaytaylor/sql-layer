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

package com.akiban.qp.physicaloperator;

public class Executable
{
    public Executable(StoreAdapter adapter, PhysicalOperator root)
    {
        this.root = root;
        this.adapter = adapter;
        this.bindings = new ArrayBindings(0); // TODO, need to make this actually usable -- maybe a ListBindings?
    }

    public Cursor cursor()
    {
        return root.cursor(adapter, bindings);
    }

    // Object state

    private final PhysicalOperator root;
    private final StoreAdapter adapter;
    private final Bindings bindings;
}
