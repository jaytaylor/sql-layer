/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.ais.model.staticgrouping;

import com.akiban.ais.model.TableName;

/**
 * Defines a group.
 *
 * For now, the only information in this is the group's name -- so it doesn't look like a very useful class! But
 * in the future we can add more information, like whether it's a group of lookup tables.
 */
public final class Group
{
    private final TableName groupName;

    public Group(TableName groupName)
    {
        if (groupName == null) {
            throw new IllegalArgumentException("group name can't be null");
        }
        this.groupName = groupName;
    }

    public TableName getGroupName()
    {
        return groupName;
    }

    Group copyButRename(TableName newName) {
        return new Group(newName);
    }

    @Override
    public String toString() {
        return Group.class.getName() + "[" + groupName + ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Group group = (Group) o;
        return groupName.equals(group.groupName);
    }

    @Override
    public int hashCode() {
        return groupName.hashCode();
    }
}
