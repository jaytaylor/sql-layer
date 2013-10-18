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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.server.service.tree.TreeCache;
import com.foundationdb.server.service.tree.TreeLink;

public class Group implements Traversable, TreeLink
{
    public static Group create(AkibanInformationSchema ais, String schemaName, String rootTableName)
    {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "Group", "schemaName");
        AISInvariants.checkNullName(rootTableName, "Group", "rootTableName");
        TableName groupName = new TableName(schemaName, rootTableName);
        AISInvariants.checkDuplicateGroups(ais, groupName);
        Group group = new Group(groupName);
        ais.addGroup(group);
        return group;
    }

    private Group(TableName name)
    {
        this.name = name;
        this.indexMap = new HashMap<>();
    }

    @Override
    public String toString()
    {
        return "Group(" + name + ")";
    }

    public TableName getName()
    {
        return name;
    }

    public String getDescription()
    {
        return name.toString();
    }

    public void setRootTable(Table rootTable)
    {
        this.rootTable = rootTable;
    }

    public Table getRoot()
    {
        return rootTable;
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
                for (IndexColumn indexColumn : index.getKeyColumns()) {
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
        GroupIndexHelper.actOnGroupIndexTables(index, GroupIndexHelper.ADD);
    }

    public void removeIndexes(Collection<GroupIndex> indexesToDrop)
    {
        indexMap.values().removeAll(indexesToDrop);
        for (GroupIndex groupIndex : indexesToDrop) {
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

    public void setTreeName(String treeName) {
        this.treeName = treeName;
    }

    private Map<String, GroupIndex> internalGetIndexMap() {
        return indexMap;
    }

    // TreeLink

    @Override
    public String getSchemaName() {
        return (rootTable != null) ? rootTable.getName().getSchemaName() : null;
    }

    @Override
    public String getTreeName() {
        return treeName;
    }

    @Override
    public void setTreeCache(TreeCache cache) {
        treeCache.set(cache);
    }

    @Override
    public TreeCache getTreeCache() {
        return treeCache.get();
    }

    // State

    private final TableName name;
    private final Map<String, GroupIndex> indexMap;
    private final AtomicReference<TreeCache> treeCache = new AtomicReference<>();
    private String treeName;
    private Table rootTable;
}
