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

public class JoinColumn
{
    public static JoinColumn create(Join join, Column parent, Column child)
    {
        return join.addJoinColumn(parent, child);
    }

    @Override
    public String toString()
    {
        return "JoinColumn(" + child.getName() + " -> " + parent.getName() + ")";
    }

    protected JoinColumn(Join join, Column parent, Column child)
    {
        this.join = join;
        this.parent = parent;
        this.child = child;
    }

    public Join getJoin()
    {
        return join;
    }

    public Column getParent()
    {
        return parent;
    }

    public Column getChild()
    {
        return child;
    }

    private final Join join;
    private final Column parent;
    private final Column child;
}
