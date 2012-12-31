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

package com.akiban.ais.model.staticgrouping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.akiban.ais.model.TableName;

/**
 * <p>Static grouping structure.</p>
 *
 * <p>By "static grouping", we mean a grouping that defines tables' hierarchies and relationships without
 * defining the tables themselves. The canonical example is "C-O-I", where we know that Customers have
 * Orders each of which have Items -- even though we don't know what the actual tables are.</p>
 *
 * <p>Within this structure, all tables are identified by {@linkplain TableName} only, and all columns
 * are identified by just their String name -- their associated table coming from context.
 *
 * <p>This class is the top level of the object relationship, which is as follows:
 * <ul>
 *  <li>A {@linkplain Grouping} object contains a list of {@link Group}s</li>
 *  <li>Each {@linkplain Group} has a name, a root table, and a list of {@link JoinDescription}s</li>
 *  <li>Each {@link JoinDescription} has:
 *   <ul>
 *    <li>The name of the child table name (<em>not</em> the parent)</li>
 *    <li>Two {@code List<String>}s with equal numbers of elements, which identify the child and
 *      parent columns that the join is on</li>
 *    <li>A [non-null, but possibly empty] list of children {@link JoinDescription}s</li>
 *   </ul>
 *  </li>
 * <ul>
 * </p>
 *
 * <p>For instance, our COI example would consist of the following structure:<pre>
 * Grouping (contains one Group):
 *   - Group "coi_schema":
 *     + root table "customers"
 *     + one child JoinDescription:
 *       - table "orders"
 *       - column pairings orders.cid references parent table's "id"
 *         (note that the parent table is known to be "customers" from context, but is not stored
 *         in this JoinDescription)
 *       - one child JoinDescription:
 *         + table "items"
 *         + column pairings items.oid references parent table's "id"
 *         + zero child JoinDescriptions
 * </pre>
 *
 * <p>As mentioned above, a {@linkplain JoinDescription} contains only the child's table name, not
 * the parent's. It is assumed that consumers of this package will have the parent's table name already,
 * since the only way to get at a {@linkplain JoinDescription} is via a {@linkplain Group} or another
 * {@linkplain JoinDescription} &emdash; each of which would be the parent. Removing the redundant
 * parent table name should reduce the opportunity for inconsistencies within the data structure.</p>
 *
 * <p>For reading a static grouping definition, the {@linkplain #traverse(GroupingVisitor)} method
 * is pretty handy. For creating or modifying a grouping definition, use {@link GroupsBuilder}.</p>
 */
public final class Grouping
{
    private static class GroupTreeNode {
        private final static List<String> ROOT_DUMMY_COLS = Arrays.asList(
                "$dummy value$"
        );
        private final JoinDescription node;
        private final List<GroupTreeNode> children = new ArrayList<GroupTreeNode>();
        private GroupTreeNode parent = null;

        static GroupTreeNode forRoot(TableName rootTable) {
            JoinDescription rootJoin = new JoinDescription(rootTable, ROOT_DUMMY_COLS, ROOT_DUMMY_COLS);
            return new GroupTreeNode(null, rootJoin);
        }

        GroupTreeNode(GroupTreeNode parent, JoinDescription node) {
            this.node = node;
            this.parent = parent;
        }

        void makeRootLike() {
            node.replaceJoinColumns(ROOT_DUMMY_COLS, ROOT_DUMMY_COLS);
            parent = null;
        }

        /**
         * Tells whether this looks like a root node. For checking integrity.
         * @param errorsOut the errors-output list
         */
        void checkRootLike(List<String> errorsOut) {
            if (parent != null) {
                errorsOut.add(this + " has non-null parent");
            }
            checkRootLikeColumns(errorsOut, node.getParentColumns());
            checkRootLikeColumns(errorsOut, node.getChildColumns());
        }

        void checkRootLikeColumns(List<String> errorsOut, List<String> columns) {
            if (!ROOT_DUMMY_COLS.toString().equals(columns.toString())) {
                errorsOut.add("columns aren't dummy-like: " + this);
            }
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(GroupTreeNode.class.getName());
            builder.append("[node ").append(node).append(" has ");
            int count = children.size();
            builder.append(count).append(count == 1 ? " child" : " children").append(']');
            return builder.toString();
        }
    }

    private String defaultSchema;
    private final Set<TableName> knownTables = new HashSet<TableName>();
    private final Map<Group,GroupTreeNode> groups = new LinkedHashMap<Group,GroupTreeNode>();

    private final VoidGroupingVisitorStub tableForgettingVisitor = new VoidGroupingVisitorStub() {
        @Override
        public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
            boolean removed = knownTables.remove(childName);
            assert removed : "couldn't remove table " + childName;
        }

        @Override
        public void visitGroup(Group group, TableName rootTable) {
            boolean removed = knownTables.remove(rootTable);
            assert removed : "couldn't remove table " + rootTable;
        }
    };

    public Grouping(String defaultSchema)
    {
        setDefaultSchema(defaultSchema);
    }

    public boolean containsTable(TableName which) {
        return knownTables.contains(which);
    }

    public boolean containsTable(String schema, String table) {
        return containsTable(TableName.create(schema, table));
    }

    public Group getGroupFor(final TableName which) {
        if (!containsTable(which)) {
            throw new IllegalArgumentException("table not in grouping: " + which);
        }
        // This is inefficient, but easy for now!
        // Eventually, we probably want to keep a map of tableName -> GroupTreeNode, so that we can get the node
        // in constant time and then just backtrack up to the parent.
        TableName groupName = traverse(new GroupingVisitorStub<TableName>(){
            private TableName found = null;
            private TableName currentGroup;

            @Override
            public void visitGroup(Group group, TableName rootTable) {
                currentGroup = group.getGroupName();
                if (which.equals(rootTable)) {
                    found = currentGroup;
                }
            }

            @Override
            public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
                if (which.equals(childName)) {
                    found = currentGroup;
                }
            }

            @Override
            public boolean startVisitingChildren() {
                return found == null;
            }

            @Override
            public TableName end() {
                return found;
            }
        });
        assert groupName != null : "null group for " + which + ": " + this;
        for (Group group : groups.keySet()) {
            if (group.getGroupName().equals(groupName)) {
                return group;
            }
        }
        throw new AssertionError("expected to find group, but didn't: for table " + which + " in " + this);
    }

    public Group getGroupFor(String schema, String table) {
        return getGroupFor(TableName.create(schema, table));
    }

    void addGroup(Group group, TableName tableName) {
        checkGroupAvailability(group.getGroupName());
        if (!knownTables.add(tableName)) {
            throw new IllegalStateException("table is already in group: " + group);
        }
        groups.put(group, GroupTreeNode.forRoot(tableName));
    }

    void dropGroup(TableName groupName) {
        for (Map.Entry<Group,GroupTreeNode> entry : groups.entrySet()) {
            Group group = entry.getKey();
            GroupTreeNode node = entry.getValue();

            if (group.getGroupName().equals(groupName)) {
                traverse(tableForgettingVisitor, node.node.getChildTableName());
                groups.remove(group);
                return;
            }
        }
        throw new IllegalArgumentException("group name not known taken: " + groupName);
    }

    void checkGroupAvailability(TableName groupName) {
        for (Group group : groups.keySet()) {
            if (group.getGroupName().equals(groupName)) {
                throw new IllegalStateException("group name already taken: " + group);
            }
        }
    }

    JoinDescription addChildTable(TableName parent, List<String> parentColumns, TableName child, List<String> childColumns) {
        if (!knownTables.contains(parent)) {
            throw new IllegalStateException("can't add child " + child.escaped()
                    + " because parent " + parent.escaped() + " isn't in grouping");
        }
        if (knownTables.contains(child)) {
            throw new IllegalStateException("can't add child " + child.escaped()
                    + " because it is already in the grouping");
        }

        GroupTreeNode node = findNode(parent);

        if (node == null) {
            throw new IllegalArgumentException("can't add child " + child.escaped()
                    + " because " + parent + " is not in grouping");
        }

        boolean addWorked = knownTables.add(child);
        assert addWorked : "failed to add child " + child + " to parent " + parent;
        JoinDescription join = new JoinDescription(child, childColumns, parentColumns);
        node.children.add(new GroupTreeNode(node, join));
        return join;
    }

    public void moveChild(String childSchema, String childTable,
                              String newParentSchema, String newParentTable,
                              List<String> childColumns, List<String> parentColumns) {
        moveChild(new TableName(childSchema, childTable), new TableName(newParentSchema, newParentTable),
                childColumns, parentColumns);
    }

    public void moveChild(TableName child, TableName newParent, List<String> childColumns, List<String> parentColumns) {
        GroupTreeNode childNode = findNode(child);
        if (childNode == null) {
            throw new IllegalArgumentException("can't move child " + child + " to parent " + newParent
                    + "because child isn't already in the grouping.");
        }
        GroupTreeNode newParentNode = findNode(newParent);
        if (newParentNode == null) {
            throw new IllegalArgumentException("can't move child " + child + " to parent " + newParent
                    + "because the new parent isn't in the grouping.");
        }

        // Make sure the parent isn't a child of the child
        GroupTreeNode genealogist = newParentNode;
        while (genealogist != null) {
            if (genealogist.node.getChildTableName().equals(child)) {
                throw new IllegalStateException("can't move child " + child + " to parent " + newParent
                    + "because new parent is currently a child of the child you want to move.");
            }
            genealogist = genealogist.parent;
        }

        childNode.node.replaceJoinColumns(parentColumns, childColumns);
        if (childNode.parent == null) {
            boolean groupRemoved = false;
            for (Map.Entry<Group, GroupTreeNode> entry : groups.entrySet()) {
                if (entry.getValue().equals(childNode)) {
                    groupRemoved = true;
                    groups.remove(entry.getKey());
                    break;
                }
            }
            assert groupRemoved : "child node "+childNode+" had no parent, but I didn't find it in groups: " + groups;
        }
        else {
            boolean childRemoved = childNode.parent.children.remove(childNode);
            assert childRemoved : "failed in removing child node " + childNode + " from parent " + childNode.parent
                    + childNode.parent.children;
        }
        childNode.parent = newParentNode;
        newParentNode.children.add(childNode);
    }

    public Group newGroupFromChild(String newRootSchema, String newRootTable, String groupName) {
        return newGroupFromChild(new TableName(newRootSchema, newRootTable), new TableName(newRootSchema, groupName));
    }

    /**
     * Promotes the given table to be the root of a new group. If the given table is already a root table, this
     * operation just renames the group.
     * @param newRoot the table to be promoted to rootness. Rootocity?
     * @param groupName the new group's name
     * @return the new Group
     */
    public Group newGroupFromChild(TableName newRoot, TableName groupName) {
        GroupTreeNode childNode = findNode(newRoot);
        if (childNode == null) {
            throw new IllegalArgumentException("can't move promote " + newRoot + " to new group " + groupName
                    + "because it isn't already in the grouping.");
        }

        checkGroupAvailability(groupName);

        if (childNode.parent == null) {
            for (Map.Entry<Group, GroupTreeNode> entry : groups.entrySet()) {
                if (entry.getValue().equals(childNode)) {
                    Group newGroup = entry.getKey().copyButRename(groupName);
                    groups.remove(entry.getKey());
                    groups.put(newGroup, entry.getValue());
                    return newGroup;
                }
            }
            throw new AssertionError("child node " + childNode
                    + " had no parent, but I couldn't find its group in " + groups);
        }

        Group ret = new Group(groupName);
        boolean removedChild = childNode.parent.children.remove(childNode);
        assert removedChild : "couldn't remove " + childNode + " from " + childNode.parent + childNode.parent.children;
        childNode.makeRootLike();

        GroupTreeNode oldNode = groups.put(ret, childNode);
        assert oldNode == null : "accidentally wrote over " + oldNode + " while creating group " + groupName;
        return ret;
    }

    public TableName getRootTable(Group group) {
        GroupTreeNode node = groups.get(group);
        if (node == null) {
            throw new IllegalArgumentException("no group called " + group);
        }
        assert node.node.getChildTableName() != null;
        return node.node.getChildTableName();
    }

    /**
     * For now, we'll do a brute-force traversal through the whole structure. If this proves unoptimal, we can replace
     * it with a map that goes from TableName to GroupTreeNode so that it's a constant-time operation.
     * @param needle the table to look up
     * @return the GroupTreeNode whose join.getChildTableName() equals the needle, or <tt>null</tt> if none was found
     */
    private GroupTreeNode findNode(TableName needle) {
        for (GroupTreeNode groupRoot : groups.values() ) {
            GroupTreeNode ret = findNode(groupRoot, needle);
            if (ret != null) {
                return ret;
            }
        }
        return null;
    }

    private GroupTreeNode findNode(GroupTreeNode root, TableName needle) {
        if (root.node.getChildTableName().equals(needle)) {
            return root;
        }
        for (GroupTreeNode child : root.children) {
            GroupTreeNode ret = findNode(child, needle);
            if (ret != null) {
                return ret;
            }
        }
        return null;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public List<Group> getGroups() {
        return new ArrayList<Group>(groups.keySet());
    }

    public List<TableName> getTables() {
        // We don't have the individual GroupTreeNodes cached in one map, so we have to traverse the tree.
        // This is a point of optimization for later.
        return traverse( new GroupingVisitorStub<List<TableName>>(){
            List<TableName> ret = new ArrayList<TableName>();

            @Override
            public void visitGroup(Group group, TableName rootTable) {
                ret.add(rootTable);
            }

            @Override
            public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
                ret.add(childName);
            }

            @Override
            public List<TableName> end() {
                return ret;
            }
        });
    }

    public void setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
    }
    
    /**
     * Traverse the entire grouping. You may not add or remove groups from within this method.
     * @param visitor the visitor to do the traversal
     * @param <T> the visitor's return value type
     * @return what the visitor's {@linkplain GroupingVisitor#end()} returns
     */
    public <T> T traverse(GroupingVisitor<T> visitor) {
        visitor.start(defaultSchema);

        for (Map.Entry<Group,GroupTreeNode> groupEntry : groups.entrySet()) {
            traverseGroup(visitor, groupEntry.getKey(), groupEntry.getValue());
        }

        return visitor.end();
    }

    /**
     * <p>Traverses the grouping starting at a given table. Depending on whether that table is a group's root, the first
     * call to the visitor will be either {@link GroupingVisitor#visitGroup(Group, TableName)} or
     * {@link GroupingVisitor#visitChild(TableName, List, TableName, List)}.
     * Either way, all subsequent tables will all be visited via
     * {@linkplain GroupingVisitor#visitChild(TableName, List, TableName, List)}. Sub-children will still be
     * descended into as with a full traversal, and your visitor's {@link GroupingVisitor#end()} method will still
     * be invoked.</p>
     *
     * You may not add or remove groups from within this method.
     * @param visitor the visitor to do the traversal
     * @param startingAt the first table to visit
     * @param <T> the visitor's return value type
     * @return what the visitor's {@linkplain GroupingVisitor#end()} returns
     */
    public <T> T traverse(GroupingVisitor<T> visitor, TableName startingAt) {
        GroupTreeNode startNode = findNode(startingAt);
        if (startNode == null) {
            throw new IllegalArgumentException("couldn't find table " + startingAt);
        }
        if (startNode.parent != null) {
            traverseChild(visitor, startNode);
            return visitor.end();
        }
        for (Map.Entry<Group,GroupTreeNode> groupEntry : groups.entrySet()) {
            if (groupEntry.getValue() == startNode) {
                return traverse(visitor, groupEntry.getValue(), groupEntry.getKey());
            }
        }
        throw new AssertionError("node "+startNode+" for " + startingAt + " not found in groups table");
    }

    /**
     * Shortcut to {@link #traverse(GroupingVisitor, com.akiban.ais.model.TableName)}
     * @param visitor the visitor
     * @param startingSchema the schema name of the starting node
     * @param startingTable the table name of the starting node
     * @param <T> the visitor's return type
     * @return the visitor's return value
     */
    public <T> T traverse(GroupingVisitor<T> visitor, String startingSchema, String startingTable) {
        return traverse(visitor, TableName.create(startingSchema, startingTable));
    }

    /**
     * Internal starting point for a traversal starting at a given node.
     * @param visitor the visitor
     * @param startNode the first node to traverse at; analagous to <tt>startingAt</tt> in
     * {@link #traverse(GroupingVisitor, TableName)}
     * @param startingGroup if the first node is a root node, that node's Group. Otherwise, this is ignored
     * @param <T> from the visitor
     * @return visitor.end()
     */
    private <T> T traverse(GroupingVisitor<T> visitor, GroupTreeNode startNode, Group startingGroup) {

        if (startNode.parent != null) {
            traverseChild(visitor, startNode);
        }
        else {
            traverseGroup(visitor, startingGroup, startNode);
        }
        return visitor.end();
    }

    /**
     * Internal recursion point for traversal into a group.
     * @param visitor the visitor
     * @param group the group's Group
     * @param groupRootNode the group's root node
     */
    private void traverseGroup(GroupingVisitor<?> visitor, Group group, GroupTreeNode groupRootNode) {
        visitor.visitGroup(group, groupRootNode.node.getChildTableName());

        if (!groupRootNode.children.isEmpty() && visitor.startVisitingChildren()) {
            for (GroupTreeNode child : groupRootNode.children) {
                traverseChild(visitor, child);
            }
            visitor.finishVisitingChildren();
        }

        visitor.finishGroup();
    }

    /**
     * Internal recursion for traversal into a branch
     * @param visitor the visitor
     * @param child the node, which must not be a group's root node.
     */
    private void traverseChild(GroupingVisitor<?> visitor, GroupTreeNode child) {
        visitor.visitChild(child.parent.node.getChildTableName(), child.node.getParentColumns(),
                child.node.getChildTableName(), child.node.getChildColumns());
        if (!child.children.isEmpty() && visitor.startVisitingChildren()) {
            for (GroupTreeNode subchild : child.children) {
                traverseChild(visitor, subchild);
            }
            visitor.finishVisitingChildren();
        }
    }

    /**
     * <p>Gets a text-based format of the grouping, which can then be parsed to create an equivalent grouping.</p>
     *
     * <p>Two groupings are equivalent if and only if their <tt>toString()</tt>s are equal.</p>
     * @return a static grouping string
     */
    @Override
    public String toString()
    {
        return traverse(VisitToString.getInstance());
    }

    /**
     * Sweeps across all our data to make sure everything is in order. This method is meant as a
     * sanity check, not as something that needs to be performed often. In fact, I only intend to
     * use it in unit tests. As such, this method is not meant to be efficient as much as clear.
     * @return this instance, so that you can call this method in a streamlined way.
     */
    Grouping checkIntegrity() {
        List<String> errors = new ArrayList<String>();
        checkGroupNames(errors);
        Set<TableName> checkKnownTables = checkGroupsTree(errors);
        if (checkKnownTables.size() != knownTables.size() || ! knownTables.containsAll(checkKnownTables)) {
            errors.add("known tables should be " + checkKnownTables + " but is " + knownTables);
        }

        if (!errors.isEmpty()) {
            StringBuilder message = new StringBuilder();
            message.append(errors.size()).append(" error");
            if (errors.size() != 1) {
                message.append('s');
            }
            message.append(":\n");
            for (String error : errors) {
                message.append("* ").append(error).append('\n');
            }
            throw new AssertionError(message);
        }
        return this;
    }

    private Set<TableName> checkGroupsTree(List<String> errors) {
        Set<TableName> seenTables = new HashSet<TableName>();
        for (GroupTreeNode root : groups.values()) {
            if (!seenTables.add(root.node.getChildTableName())) {
                errors.add("duplicate table name: " + root);
            }
            root.checkRootLike(errors);
            for (GroupTreeNode child : root.children) {
                checkGroupsBranch(errors, seenTables, root, child);
            }
        }
        return seenTables;
    }

    private void checkGroupsBranch(List<String> errors, Set<TableName> seenTables,
                                   GroupTreeNode parent, GroupTreeNode root)
    {
        if (!seenTables.add(root.node.getChildTableName())) {
            errors.add("duplicate table name: " + root);
        }
        if (root.parent == null) {
            errors.add("null parent for " + root);
        }
        else if (!root.parent.equals(parent)){
            errors.add("expected parent " + parent + " for " + root);
        }
        for (GroupTreeNode child : root.children) {
            checkGroupsBranch(errors, seenTables, root, child);
        }
    }

    private void checkGroupNames(List<String> errors) {
        boolean foundProblem = false;
        for (Group group : groups.keySet()) {
            if (group == null) {
                errors.add("null group");
                foundProblem = true;
            }
            else if (group.getGroupName() == null) {
                errors.add("null group name");
                foundProblem = true;
            }
            else if (group.getGroupName().getTableName().trim().length() == 0) {
                errors.add("empty group name");
                foundProblem = true;
            }
        }
        if (foundProblem) {
            errors.add("    groups: " + groups);
        }
    }
}
