/* <GENERIC-HEADER - BEGIN>
 *
 * $(COMPANY) $(COPYRIGHT)
 *
 * Created on: Nov, 20, 2009
 * Created by: Thomas Hazel
 *
 * </GENERIC-HEADER - END> */

package com.akiban.ais.model;

import java.io.Serializable;
import java.util.*;

public class AkibaInformationSchema implements Serializable, Traversable
{
    public AkibaInformationSchema()
    {
        for (Type type : Types.types()) {
            addType(type);
        }
        charsetAndCollation = CharsetAndCollation.intern(DEFAULT_CHARSET, DEFAULT_COLLATION);
    }

    public AkibaInformationSchema(AkibaInformationSchema ais)
    {
        this();
        groups.putAll(ais.getGroups());
        userTables.putAll(ais.getUserTables());
        groupTables.putAll(ais.getGroupTables());
        joins.putAll(ais.getJoins());
    }


    // AkibaInformationSchema interface

    public String getDescription()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append("AkibaInformationSchema(");

        boolean first = true;
        for (Group group : groups.values()) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }

            buffer.append(group.getDescription());
        }

        buffer.append(")");
        return buffer.toString();
    }

    public Group getGroup(final String groupName)
    {
        return groups.get(groupName);
    }

    public Map<String, Group> getGroups()
    {
        return groups;
    }

    public Map<TableName, UserTable> getUserTables()
    {
        return userTables;
    }

    public Map<TableName, GroupTable> getGroupTables()
    {
        return groupTables;
    }

    public Table getTable(String schemaName, String tableName)
    {
        Table table = getUserTable(schemaName, tableName);
        if (table == null) {
            table = getGroupTable(schemaName, tableName);
        }

        return table;
    }

    public Table getTable(TableName tableName)
    {
        Table table = getUserTable(tableName);
        if (table == null) {
            table = getGroupTable(tableName);
        }

        return table;
    }

    public UserTable getUserTable(final String schemaName, final String tableName)
    {
        return getUserTable(new TableName(schemaName, tableName));
    }

    public UserTable getUserTable(final TableName tableName)
    {
        return userTables.get(tableName);
    }

    public GroupTable getGroupTable(final String schemaName, final String tableName)
    {
        return getGroupTable(new TableName(schemaName, tableName));
    }

    public GroupTable getGroupTable(final TableName tableName)
    {
        return groupTables.get(tableName);
    }

    public Collection<Type> getTypes()
    {
        return types.values();
    }

    public Type getType(String typename)
    {
        return types.get(normalizeTypename(typename));
    }

    public Map<String, Join> getJoins()
    {
        return joins;
    }

    public Join getJoin(String joinName)
    {
        return joins.get(joinName);
    }

    public CharsetAndCollation getCharsetAndCollation()
    {
        return charsetAndCollation;
    }

    @Override
    public void traversePreOrder(Visitor visitor) throws Exception
    {
        for (Type type : types.values()) {
            visitor.visitType(type);
        }
        for (UserTable userTable : userTables.values()) {
            visitor.visitUserTable(userTable);
            userTable.traversePreOrder(visitor);
        }
        for (Join join : joins.values()) {
            visitor.visitJoin(join);
            join.traversePreOrder(visitor);
        }
        for (GroupTable groupTable : groupTables.values()) {
            visitor.visitGroupTable(groupTable);
            groupTable.traversePreOrder(visitor);
        }
        for (Group group : groups.values()) {
            visitor.visitGroup(group);
        }
    }

    @Override
    public void traversePostOrder(Visitor visitor) throws Exception
    {
        for (Type type : types.values()) {
            visitor.visitType(type);
        }
        for (UserTable userTable : userTables.values()) {
            userTable.traversePostOrder(visitor);
            visitor.visitUserTable(userTable);
        }
        for (Join join : joins.values()) {
            join.traversePreOrder(visitor);
            visitor.visitJoin(join);
        }
        for (GroupTable groupTable : groupTables.values()) {
            groupTable.traversePostOrder(visitor);
            visitor.visitGroupTable(groupTable);
        }
        for (Group group : groups.values()) {
            visitor.visitGroup(group);
        }
    }

    // AkibaInformationSchema interface

    public void addGroup(Group group)
    {
        groups.put(group.getName(), group);
    }

    public void addUserTable(UserTable table)
    {
        userTables.put(table.getName(), table);
    }

    public void addGroupTable(GroupTable table)
    {
        groupTables.put(table.getName(), table);
    }

    public void addType(Type type)
    {
        final String normal = normalizeTypename(type.name());

        final Type oldType = types.get(normal);

        // TODO - remove once C++ code has new encoding attribute
        if (oldType != null) {
            return;
        }

        // TODO - rethink why the types are a static element of an
        // AIS.
        if (oldType != null && !type.equals(oldType)) {
            throw new IllegalStateException("Attempting to add an incompatible Type");
        }

        types.put(normal, type);
    }

    public void addJoin(Join join)
    {
        joins.put(join.getName(), join);
    }

    public void deleteGroupAndGroupTable(Group group)
    {
        Group removedGroup = groups.remove(group.getName());
        assert removedGroup == group;
        GroupTable groupTable = group.getGroupTable();
        assert groupTable.getRoot() == null;
        GroupTable removedGroupTable = groupTables.remove(groupTable.getName());
        assert removedGroupTable == groupTable;
    }

    private String normalizeTypename(String typename)
    {
        typename = typename.toLowerCase();
        StringBuilder buffer = new StringBuilder();
        boolean first = true;
        for (String token : tokens(typename)) {
            if (!(token.equals("precision") || token.equals("signed"))) {
                if (first) {
                    first = false;
                } else {
                    buffer.append(' ');
                }
                if (token.equals("integer")) {
                    token = "int";
                } else if (token.equals("numeric") || token.equals("dec")) {
                    token = "decimal";
                }
                buffer.append(token);
            }
        }
        typename = buffer.toString();
        return typename;
    }

    private List<String> tokens(String x)
    {
        x += ' '; // append whitespace to sify loop termination
        List<String> tokens = new ArrayList<String>();
        boolean inWhitespace = true;
        StringBuilder token = new StringBuilder();
        for (int i = 0; i < x.length(); i++) {
            char c = x.charAt(i);
            if (inWhitespace) {
                if (whitespace(c)) {
                    // Nothing to do
                } else {
                    token.append(c);
                    inWhitespace = false;
                }
            } else {
                if (whitespace(c)) {
                    tokens.add(token.toString());
                    token.setLength(0);
                    inWhitespace = true;
                } else {
                    token.append(c);
                }
            }
        }
        return tokens;
    }

    private boolean whitespace(char c)
    {
        return c == ' ' || c == '\n' || c == '\t' || c == '\r';
    }

    private void checkGroups(List<String> out)
    {
        for (Map.Entry<String,Group> entry : groups.entrySet())
        {
            String name = entry.getKey();
            Group group = entry.getValue();
            if (group == null) {
                out.add("null group for name: " + name);
            }
            else if (name == null) {
                out.add("null name detected");
            }
            else if (!name.equals(group.getName())) {
                out.add("name mismatch, expected <" + name + "> for group " + group);
            }
            GroupTable groupTable = group.getGroupTable();
            if (groupTable == null) {
                out.add("null group table for group: " + name);
            }
            else if (!groupTables.containsKey(groupTable.getName())) {
                out.add("group tables didn't contain group's getGroupTable(): " + groupTable.getName());
            }
        }
    }

    private void checkTables(List<String> out, Map<TableName, ? extends Table> tables,
                             boolean isUserTable, Set<TableName> seenTables)
    {
        for (Map.Entry<TableName, ? extends Table> entry : tables.entrySet())
        {
            TableName tableName = entry.getKey();
            Table table = entry.getValue();
            if (table == null) {
                out.add("null table for name: " + tableName);
            }
            else if (tableName == null) {
                out.add("null table name detected");
            }
            else if (!tableName.equals(table.getName())) {
                out.add("name mismatch, expected <" + tableName + "> for table " + table);
            }
            else if(table.isGroupTable() == isUserTable) {
                out.add("wrong value for isGroupTable(): " + tableName);
            }
            else if (table.isUserTable() != isUserTable) {
                out.add("wrong value for isUserTable(): " + tableName);
            }
            else if (!seenTables.add(tableName)) {
                out.add("duplicate table name: " + tableName);
            }
            else if (table.getAIS() != this) {
                out.add("AIS self-reference failure");
            }
            else {
                table.checkIntegrity(out);
            }
        }
    }

    private void checkJoins(List<String> out)
    {
        for (Map.Entry<String,Join> entry : joins.entrySet())
        {
            String name = entry.getKey();
            Join join = entry.getValue();
            if (join == null) {
                out.add("null join for name: " + name);
            }
            else if (name == null) {
                out.add("null join name detected");
            }
            else if(!name.equals(join.getName())) {
                out.add("name mismatch, expected <" + name + "> for join: " + join);
            }
            else if(join.checkIntegrity(out))
            {
                UserTable child = join.getChild();
                UserTable parent = join.getParent();
                if (!userTables.containsKey(child.getName())) {
                    out.add("child not in user tables list: " + child.getName());
                }
                else if (!userTables.containsKey(parent.getName())) {
                    out.add("parent not in user tables list: " + child.getName());
                }
                else if (join.getAIS() != this) {
                    out.add("AIS self-reference failure");
                }
            }
        }
    }

    private void checkTypesNames(List<String> out)
    {
        for (Map.Entry<String,Type> entry : types.entrySet())
        {
            String name = entry.getKey();
            Type type = entry.getValue();
            if (type == null) {
                out.add("null type for name: " + name);
            }
            else if (name == null) {
                out.add("null type name detected");
            }
            else if (!name.equals(type.name())) {
                out.add("name mismatch, expected <" + name + "> for type: " + type);
            }
        }
    }

    /**
     * Checks the AIS's integrity; that everything is internally consistent.
     * @throws IllegalStateException if anything isn't consistent
     */
    public void checkIntegrity()
    {
        List<String> problems = new LinkedList<String>();
        try
        {
            checkIntegrity(problems);
        }
        catch (Throwable t)
        {
            throw new IllegalStateException("exception thrown while trying to check AIS integrity", t);
        }
        if (!problems.isEmpty())
        {
            throw new IllegalStateException("AIS integrity failed: " + problems);
        }
    }

    /**
     * Checks the AIS's integrity; that everything is internally consistent.
     * @param out the list into which error messages should go
     * @throws IllegalStateException if anything isn't consistent
     */
    public void checkIntegrity(List<String> out) throws IllegalStateException
    {
        checkGroups(out);
        Set<TableName> seenTables = new HashSet<TableName>(userTables.size() + groupTables.size(), 1.0f);
        checkTables(out, userTables, true, seenTables);
        checkTables(out, groupTables, false, seenTables);
        checkJoins(out);
        checkTypesNames(out);
    }

    // State

    public static final String DEFAULT_CHARSET = "latin1";
    public static final String DEFAULT_COLLATION = "latin1_swedish_ci";

    private Map<String, Group> groups = new TreeMap<String, Group>();
    private Map<TableName, UserTable> userTables = new TreeMap<TableName, UserTable>();
    private Map<TableName, GroupTable> groupTables = new TreeMap<TableName, GroupTable>();
    private Map<String, Join> joins = new TreeMap<String, Join>();
    private Map<String, Type> types = new TreeMap<String, Type>();
    private CharsetAndCollation charsetAndCollation;
}
