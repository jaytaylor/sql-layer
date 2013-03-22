
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
