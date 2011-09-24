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

package com.akiban.qp.operator;

public abstract class Bindings {
    /**
     * Gets the object bound to the given index.
     * @param index the index to look up
     * @return the object at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public abstract Object get(int index);

    /**
     * Bind a value to the given index. Used for variables introduced by execution plan, not for query parameters.
     * @param index the index to set
     * @param value the value to assign
     */
    public abstract void set(int index, Object value);
}
