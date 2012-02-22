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

package com.akiban.ais.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.akiban.ais.model.validation.AISInvariants;

public class Group implements Traversable
{
    public static Group create(AkibanInformationSchema ais, String groupName)
    {
        ais.checkMutability();
        AISInvariants.checkDuplicateGroups(ais, groupName);
        Group group = new Group(groupName);
        ais.addGroup(group);
        return group;
    }

    public Group(final String name)
    {
        AISInvariants.checkNullName(name, "Group", "group name");
        this.name = name;
        this.indexMap = new HashMap<String, GroupIndex>();
    }

    @Override
    public String toString()
    {
        return "Group(" + name + " -> " + groupTable.getName() + ")";
    }

    public String getName()
    {
        return name;
    }

    public String getDescription()
    {
        return name;
    }

    public GroupTable getGroupTable()
    {
        return groupTable;
    }

    public void setGroupTable(GroupTable groupTable)
    {
        this.groupTable = groupTable;
    }

    public Collection<GroupIndex> getIndexes()
    {
        return Collections.unmodifiableCollection(internalGetIndexMap().values());
    }

    public GroupIndex getIndex(String indexName)
    {
        return internalGetIndexMap().get(indexName.toLowerCase());
    }

    public void checkIntegrity(List<String> out)
    {
        for (Map.Entry<String, GroupIndex> entry : internalGetIndexMap().entrySet()) {
            String name = entry.getKey();
            GroupIndex index = entry.getValue();
            if (name == null) {
                out.add("null name for index: " + index);
            } else if (index == null) {
                out.add("null index for name: " + name);
            } else if (index.getGroup() != this) {
                out.add("group's index.getGroup() wasn't the group" + index + " <--> " + this);
            }
            if (index != null) {
                for (IndexColumn indexColumn : index.getColumns()) {
                    if (!index.equals(indexColumn.getIndex())) {
                        out.add("index's indexColumn.getIndex() wasn't index: " + indexColumn);
                    }
                    Column column = indexColumn.getColumn();
                    if (column == null) {
                        out.add("column was null in index column: " + indexColumn);
                    }
                    else if(column.getTable() == null) {
                        out.add("column's table was null: " + column);
                    }
                    else if(column.getTable().getGroup() != this) {
                        out.add("column table's group was wrong " + column.getTable().getGroup() + "<-->" + this);
                    }
                }
            }
        }
    }

    public void addIndex(GroupIndex index)
    {
        indexMap.put(index.getIndexName().getName().toLowerCase(), index);
        groupTable.addGroupIndex(index);
        GroupIndexHelper.actOnGroupIndexTables(index, GroupIndexHelper.ADD);
    }

    public void removeIndexes(Collection<GroupIndex> indexesToDrop)
    {
        indexMap.values().removeAll(indexesToDrop);
        for (GroupIndex groupIndex : indexesToDrop) {
            groupTable.removeGroupIndex(groupIndex);
            GroupIndexHelper.actOnGroupIndexTables(groupIndex, GroupIndexHelper.REMOVE);
        }
    }

    public void traversePreOrder(Visitor visitor)
    {
        for (Index index : getIndexes()) {
            visitor.visitIndex(index);
            index.traversePreOrder(visitor);
        }
    }

    public void traversePostOrder(Visitor visitor)
    {
        for (Index index : getIndexes()) {
            index.traversePostOrder(visitor);
            visitor.visitIndex(index);
        }
    }

    private Map<String, GroupIndex> internalGetIndexMap() {
        return indexMap;
    }

    // State

    private final String name;
    private GroupTable groupTable;
    private final Map<String, GroupIndex> indexMap;
}
