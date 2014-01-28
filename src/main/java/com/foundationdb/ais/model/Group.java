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
import java.util.Map;

import com.foundationdb.ais.model.validation.AISInvariants;

public class Group extends HasStorage implements Visitable
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
        return internalGetIndexMap().get(indexName);
    }

    public void addIndex(GroupIndex index)
    {
        indexMap.put(index.getIndexName().getName(), index);
        GroupIndexHelper.actOnGroupIndexTables(index, GroupIndexHelper.ADD);
    }

    public void removeIndexes(Collection<GroupIndex> indexesToDrop)
    {
        indexMap.values().removeAll(indexesToDrop);
        for (GroupIndex groupIndex : indexesToDrop) {
            GroupIndexHelper.actOnGroupIndexTables(groupIndex, GroupIndexHelper.REMOVE);
        }
    }

    private Map<String, GroupIndex> internalGetIndexMap() {
        return indexMap;
    }

    public boolean hasMemoryTableFactory()
    {
        return (storageDescription != null) && storageDescription.isMemoryTableFactory();
    }

    // HasStorage

    @Override
    public AkibanInformationSchema getAIS() {
        return rootTable.getAIS();
    }

    @Override
    public String getTypeString() {
        return "Group";
    }

    @Override
    public String getNameString() {
        return name.toString();
    }

    @Override
    public String getSchemaName() {
        return (rootTable != null) ? rootTable.getName().getSchemaName() : null;
    }

    // Visitable

    /** Visit this instance, the root table and then all group indexes. */
    @Override
    public void visit(Visitor visitor) {
        visitor.visit(this);
        rootTable.visit(visitor);
        for(Index i : getIndexes()) {
            i.visit(visitor);
        }
    }

    // State

    private final TableName name;
    private final Map<String, GroupIndex> indexMap;
    private Table rootTable;
}
