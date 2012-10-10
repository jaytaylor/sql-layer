/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.ais.model.validation.AISInvariants;
import com.akiban.server.service.tree.TreeCache;
import com.akiban.server.service.tree.TreeLink;

public class Group implements Traversable, TreeLink
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
        TableName tableName = (rootTable != null) ? rootTable.getName() : null;
        return "Group(" + name + " -> " + tableName + ")";
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

    public void setRootTable(UserTable rootTable)
    {
        this.rootTable = rootTable;
    }

    public UserTable getRoot()
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

    private final String name;
    private final Map<String, GroupIndex> indexMap;
    private final AtomicReference<TreeCache> treeCache = new AtomicReference<TreeCache>();
    private String treeName;
    private UserTable rootTable;
    private GroupTable groupTable;
}
