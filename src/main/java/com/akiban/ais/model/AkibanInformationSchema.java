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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.akiban.ais.model.validation.AISValidation;
import com.akiban.ais.model.validation.AISValidationFailure;
import com.akiban.ais.model.validation.AISValidationOutput;
import com.akiban.ais.model.validation.AISValidationResults;

public class AkibanInformationSchema implements Traversable
{
    public static String getDefaultCharset() {
        return defaultCharset;
    }
    public static String getDefaultCollation() {
        return defaultCollation;
    }
    public static void setDefaultCharsetAndCollation(String charset, String collation) {
        defaultCharset = charset;
        defaultCollation = collation;
    }

    public AkibanInformationSchema()
    {
        for (Type type : Types.types()) {
            addType(type);
        }
        charsetAndCollation = CharsetAndCollation.intern(defaultCharset, defaultCollation);
    }


    // AkibanInformationSchema interface

    public String getDescription()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append("AkibanInformationSchema(");

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

    /** @deprecated Use the fully qualified version {@link #getGroup(TableName)} **/
    public Group getGroup(final String groupName)
    {
        Group candidate = null;
        for(Group group : groups.values()) {
            if(group.getName().getTableName().equals(groupName)) {
                if(candidate != null) {
                    throw new IllegalArgumentException("Ambiguous group name: " + groupName);
                }
                candidate = group;
            }
        }
        return candidate;
    }

    public Group getGroup(final TableName groupName)
    {
        return groups.get(groupName);
    }

    public Map<TableName, Group> getGroups()
    {
        return groups;
    }

    public Map<TableName, UserTable> getUserTables()
    {
        return userTables;
    }

    public void removeGroup(Group group) {
        groups.remove(group.getName());
    }

    public Table getTable(String schemaName, String tableName)
    {
        return getUserTable(schemaName, tableName);
    }

    public Table getTable(TableName tableName)
    {
        return getUserTable(tableName);
    }

    public UserTable getUserTable(final String schemaName, final String tableName)
    {
        return getUserTable(new TableName(schemaName, tableName));
    }

    public UserTable getUserTable(final TableName tableName)
    {
        return userTables.get(tableName);
    }

    public synchronized UserTable getUserTable(int tableId)
    {
        ensureTableIdLookup();
        return userTablesById.get(tableId);
    }

    public Map<TableName, View> getViews()
    {
        return views;
    }

    public View getView(final String schemaName, final String tableName)
    {
        return getView(new TableName(schemaName, tableName));
    }

    public View getView(final TableName tableName)
    {
        return views.get(tableName);
    }

    public Columnar getColumnar(String schemaName, String tableName)
    {
        Columnar columnar = getTable(schemaName, tableName);
        if (columnar == null) {
            columnar = getView(schemaName, tableName);
        }
        return columnar;
    }

    public Columnar getColumnar(TableName tableName)
    {
        Columnar columnar = getTable(tableName);
        if (columnar == null) {
            columnar = getView(tableName);
        }
        return columnar;
    }

    public Collection<Type> getTypes()
    {
        return types.values();
    }

    public Type getType(String typename)
    {
        return types.get(normalizeTypename(typename));
    }

    public boolean isTypeSupported(String typename)
    {
        final Type type = getType(typename);
        return !Types.unsupportedTypes().contains(type);
    }

    public boolean isTypeSupportedAsIndex(String typename)
    {
        final Type type = getType(typename);
        return !Types.unsupportedTypes().contains(type) &&
               !Types.unsupportedIndexTypes().contains(type);
    }

    public boolean canTypesBeJoined(String typeName1, String typeName2) {
        Type t1 = getType(typeName1);
        Type t2 = getType(typeName2);
        // Encoding equal or both int types
        return (t1 != null) && (t2 != null) &&
               (t1.encoding().equals(t2.encoding()) ||
                (Types.isIntType(t1) && Types.isIntType(t2)));
    }

    public Map<String, Join> getJoins()
    {
        return joins;
    }

    public Join getJoin(String joinName)
    {
        return joins.get(joinName);
    }

    public Map<String, Schema> getSchemas()
    {
        return schemas;
    }

    public Schema getSchema(String schema)
    {
        return schemas.get(schema);
    }

    public Map<TableName, Sequence> getSequences()
    {
        return sequences;
    }
    
    public Sequence getSequence (final TableName sequenceName)
    {
        return sequences.get(sequenceName);
    }
    
    public Map<TableName, Routine> getRoutines()
    {
        return routines;
    }
    
    public Routine getRoutine(final String schemaName, final String routineName)
    {
        return getRoutine(new TableName(schemaName, routineName));
    }
    
    public Routine getRoutine(final TableName routineName)
    {
        return routines.get(routineName);
    }
    
    public Map<TableName, SQLJJar> getSQLJJars() {
        return sqljJars;
    }
    
    public SQLJJar getSQLJJar(final String schemaName, final String jarName)
    {
        return getSQLJJar(new TableName(schemaName, jarName));
    }
    
    public SQLJJar getSQLJJar(final TableName name)
    {
        return sqljJars.get(name);
    }
    
    public CharsetAndCollation getCharsetAndCollation()
    {
        return charsetAndCollation;
    }

    @Override
    public void traversePreOrder(Visitor visitor)
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
        for (Group group : groups.values()) {
            visitor.visitGroup(group);
            group.traversePreOrder(visitor);
        }
    }

    @Override
    public void traversePostOrder(Visitor visitor)
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
        for (Group group : groups.values()) {
            group.traversePostOrder(visitor);
            visitor.visitGroup(group);
        }
    }

    // AkibanInformationSchema interface

    public void addGroup(Group group)
    {
        groups.put(group.getName(), group);
    }

    public void addUserTable(UserTable table)
    {
        TableName tableName = table.getName();
        userTables.put(tableName, table);

        // TODO: Create on demand until Schema is more of a first class citizen
        Schema schema = getSchema(tableName.getSchemaName());
        if (schema == null) {
            schema = new Schema(tableName.getSchemaName());
            addSchema(schema);
        }
        schema.addUserTable(table);
    }

    public void addView(View view)
    {
        TableName viewName = view.getName();
        views.put(viewName, view);

        Schema schema = getSchema(viewName.getSchemaName());
        if (schema == null) {
            schema = new Schema(viewName.getSchemaName());
            addSchema(schema);
        }
        schema.addView(view);
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

    public void addSchema(Schema schema)
    {
        schemas.put(schema.getName(), schema);
    }

    public void addSequence (Sequence seq)
    {
        TableName sequenceName = seq.getSequenceName();
        sequences.put(sequenceName, seq);

        // TODO: Create on demand until Schema is more of a first class citizen
        Schema schema = getSchema(sequenceName.getSchemaName());
        if (schema == null) {
            schema = new Schema(sequenceName.getSchemaName());
            addSchema(schema);
        }
        schema.addSequence(seq);
    }
    
    public void addRoutine(Routine routine)
    {
        TableName routineName = routine.getName();
        routines.put(routineName, routine);

        // TODO: Create on demand until Schema is more of a first class citizen
        Schema schema = getSchema(routineName.getSchemaName());
        if (schema == null) {
            schema = new Schema(routineName.getSchemaName());
            addSchema(schema);
        }
        schema.addRoutine(routine);
    }
    
    public void addSQLJJar(SQLJJar sqljJar) {
        TableName jarName = sqljJar.getName();
        sqljJars.put(jarName, sqljJar);

        // TODO: Create on demand until Schema is more of a first class citizen
        Schema schema = getSchema(jarName.getSchemaName());
        if (schema == null) {
            schema = new Schema(jarName.getSchemaName());
            addSchema(schema);
        }
        schema.addSQLJJar(sqljJar);
    }

    public void deleteGroup(Group group)
    {
        Group removedGroup = groups.remove(group.getName());
        assert removedGroup == group;
    }

    private String normalizeTypename(String typename)
    {
        // Remove leading whitespace, collapse multiple whitespace, lowercase
        return typename.trim().replaceAll("\\s+", " ").toLowerCase();
    }

    /**
     * @deprecated - use {@link #validate(Collection)}
     * @param out
     */
    private void checkGroups(List<String> out)
    {
        for (Map.Entry<TableName,Group> entry : groups.entrySet())
        {
            TableName name = entry.getKey();
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
            else {
                group.checkIntegrity(out);
            }
        }
    }

    /**
     * @deprecated - use {@link #validate(Collection)}
     * @param out
     * @param tables
     * @param isUserTable
     * @param seenTables
     */
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
    /**
     * @deprecated - use {@link #validate(Collection)} 
     * @param out
     */
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

    /**
     * @deprecated use {@link #validate(Collection)}
     * @param out
     */
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
     * @deprecated - use {@link #validate(Collection)}
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
     * @deprecated use {@link #validate(Collection)}
     *
     */
    public void checkIntegrity(List<String> out) throws IllegalStateException
    {
        checkGroups(out);
        Set<TableName> seenTables = new HashSet<TableName>(userTables.size(), 1.0f);
        checkTables(out, userTables, true, seenTables);
        checkJoins(out);
        checkTypesNames(out);
    }

    /**
     * Validates this AIS against the given validations. All validations will run, even if one fails (unless any
     * throw an unchecked exception).
     * @param validations the validations to run
     * @return the result of the validations
     */
   public AISValidationResults validate(Collection<AISValidation> validations) {
       AISFailureList validationFailures = new AISFailureList();
       for (AISValidation v : validations) {
           v.validate(this, validationFailures);
       }
       return validationFailures; 
   }

   /**
    * Marks this AIS as frozen; it is now immutable, and any safe publication to another thread will guarantee
    * that the AIS will not change from under that thread.
    */
   public void freeze() {
       isFrozen = true; 
       //TDOO: any other required code?
   }
   
   public boolean isFrozen() {
       return isFrozen;
   }

   /** For use within the AIS package; throws an exception if isFrozen is false */
   void checkMutability() throws IllegalStateException {
       if (isFrozen) {
           throw new IllegalStateException ("Attempting to modify a frozen AIS");
       }
   }

    synchronized void invalidateTableIdMap()
    {
        userTablesById = null;
    }

    private void ensureTableIdLookup()
    {
        if (userTablesById == null) {
            userTablesById = new HashMap<Integer, UserTable>();
            for (UserTable userTable : userTables.values()) {
                userTablesById.put(userTable.getTableId(), userTable);
            }
        }
    }

    void removeTable(TableName name) {
        userTables.remove(name);
        Schema schema = getSchema(name.getSchemaName());
        if (schema != null) {
            schema.removeTable(name.getTableName());
        }
        invalidateTableIdMap();
    }
    
    void removeSequence (TableName name) {
        sequences.remove(name);
        Schema schema = getSchema(name.getSchemaName());
        if (schema != null) {
            schema.removeSequence(name.getTableName());
        }
    }

    public void removeRoutine(TableName name) {
        routines.remove(name);
        Schema schema = getSchema(name.getSchemaName());
        if (schema != null) {
            schema.removeRoutine(name.getTableName());
        }
    }

    public void removeView(TableName name) {
        views.remove(name);
        Schema schema = getSchema(name.getSchemaName());
        if (schema != null) {
            schema.removeView(name.getTableName());
        }
    }

    public void removeSQLJJar(TableName jarName) {
        sqljJars.remove(jarName);
        Schema schema = getSchema(jarName.getSchemaName());
        if (schema != null) {
            schema.removeSQLJJar(jarName.getTableName());
        }
    }

    public long getGeneration() {
        return generation;
    }

    public void setGeneration(long generation) {
        if(this.generation != -1) { // TODO: Cleanup. Ideally add generation to constructor
            checkMutability();
        }
        this.generation = generation;
    }

    @SuppressWarnings("unchecked") // Expected, value type varies
    public <V> V getCachedValue(Object key, CacheValueGenerator<V> generator) {
        Object val = cachedValues.get(key);
        if(val == null) {
            val = generator.valueFor(this);
            Object firstVal = cachedValues.putIfAbsent(key, val);
            if(firstVal != null) {
                val = firstVal; // Someone got here before, use theirs
            }
        }
        return (V)val;
    }

    @Override
    public String toString() {
        return "AIS(" + generation + ")";
    }


    // State

    private static String defaultCharset = "utf8";
    private static String defaultCollation = "utf8_bin";

    private final Map<TableName, Group> groups = new TreeMap<TableName, Group>();
    private final Map<TableName, UserTable> userTables = new TreeMap<TableName, UserTable>();
    private final Map<TableName, Sequence> sequences = new TreeMap<TableName, Sequence>();
    private final Map<TableName, View> views = new TreeMap<TableName, View>();
    private final Map<TableName, Routine> routines = new TreeMap<TableName, Routine>();
    private final Map<TableName, SQLJJar> sqljJars = new TreeMap<TableName, SQLJJar>();
    private final Map<String, Join> joins = new TreeMap<String, Join>();
    private final Map<String, Type> types = new TreeMap<String, Type>();
    private final Map<String, Schema> schemas = new TreeMap<String, Schema>();
    private final CharsetAndCollation charsetAndCollation;
    private final ConcurrentMap cachedValues = new ConcurrentHashMap(4,0.75f,4); // Very few, write-once entries expected
    private long generation = -1;

    private Map<Integer, UserTable> userTablesById = null;
    private boolean isFrozen = false;

    private static class AISFailureList extends AISValidationResults implements AISValidationOutput {

        @Override
        public void reportFailure(AISValidationFailure failure) {
            if (failure != null) {
                failureList.add(failure);
            }
        }
        public AISFailureList() {
            failureList = new LinkedList<AISValidationFailure>();
        }
    }
}
