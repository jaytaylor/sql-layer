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

import com.foundationdb.ais.model.validation.AISValidation;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.model.validation.AISValidationResults;
import com.foundationdb.server.types.common.types.StringFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AkibanInformationSchema implements Visitable
{
    public static String getDefaultCharsetName() {
        return defaultCharsetName;
    }

    public static String getDefaultCollationName() {
        return defaultCollationName;
    }

    public static int getDefaultCharsetId() {
        return StringFactory.charsetNameToId(defaultCharsetName);
    }

    public static int getDefaultCollationId() {
        return StringFactory.collationNameToId(defaultCollationName);
    }

    public static void setDefaultCharsetAndCollation(String charsetName, String collationName) {
        defaultCharsetName = charsetName;
        defaultCollationName = collationName;
    }

    public AkibanInformationSchema()
    {
        charsetId = getDefaultCharsetId();
        collationId = getDefaultCollationId();
    }

    public AkibanInformationSchema(int generation) {
        this();
        this.generation = generation;
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

    /** deprecate? Use the fully qualified version {@link #getGroup(TableName)} **/
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

    public Map<TableName, Table> getTables()
    {
        return tables;
    }

    public void removeGroup(Group group) {
        groups.remove(group.getName());
    }

    public Table getTable(final String schemaName, final String tableName)
    {
        return getTable(new TableName(schemaName, tableName));
    }

    public Table getTable(final TableName tableName)
    {
        return tables.get(tableName);
    }

    public synchronized Table getTable(int tableId)
    {
        ensureTableIdLookup();
        return tablesById.get(tableId);
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
    
    public int getCharsetId()
    {
        return charsetId;
    }

    public String getCharsetName()
    {
        return StringFactory.charsetIdToName(charsetId);
    }

    public int getCollationId()
    {
        return collationId;
    }

    public String getCollationName()
    {
        return StringFactory.collationIdToName(collationId);
    }
    
    public Map<TableName, Constraint> getConstraints() {
        return constraints;
    }
    
    public Constraint getConstraint(final TableName constraintName) {
        return constraints.get(constraintName);
    }

    // AkibanInformationSchema interface

    public void addGroup(Group group)
    {
        groups.put(group.getName(), group);
    }

    public void addTable(Table table)
    {
        TableName tableName = table.getName();
        tables.put(tableName, table);

        // TODO: Create on demand until Schema is more of a first class citizen
        Schema schema = getSchema(tableName.getSchemaName());
        if (schema == null) {
            schema = new Schema(tableName.getSchemaName());
            addSchema(schema);
        }
        schema.addTable(table);
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
    
    public void addConstraint(Constraint constraint){
        TableName constraintName = constraint.getConstraintName();
        constraints.put(constraintName, constraint);
        Schema schema = getSchema(constraintName.getSchemaName());
        if(schema == null) {
            schema = new Schema(constraintName.getSchemaName());
            addSchema(schema);
        }
        schema.addConstraint(constraint);
    }
    
    public void deleteGroup(Group group)
    {
        Group removedGroup = groups.remove(group.getName());
        assert removedGroup == group;
    }

    /**
     * Validates this AIS against the given validations. All validations will run, even if one fails (unless any
     * throw an unchecked exception).
     * @param validations the validations to run
     * @return the result of the validations
     */
   public AISValidationResults validate(Collection<? extends AISValidation> validations) {
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
        tablesById = null;
    }

    private void ensureTableIdLookup()
    {
        if (tablesById == null) {
            tablesById = new HashMap<>();
            for (Table table : tables.values()) {
                tablesById.put(table.getTableId(), table);
            }
        }
    }

    void removeTable(TableName name) {
        tables.remove(name);
        Schema schema = getSchema(name.getSchemaName());
        if (schema != null) {
            schema.removeTable(name.getTableName());
        }
        invalidateTableIdMap();
    }
    
    public void removeSequence (TableName name) {
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
    public void removeConstraint(TableName name) {
        constraints.remove(name);
        Schema schema = getSchema(name.getSchemaName());
        if (schema != null) {
            schema.removeConstraint(name.getTableName());
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
        if(val == null && generator != null) {
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

    // Visitable

    /** Visit every group. */
    @Override
    public void visit(Visitor visitor) {
        for(Group g : groups.values()) {
            g.visit(visitor);
        }
    }


    // State

    private static String defaultCharsetName = "utf8";
    private static String defaultCollationName = "UCS_BINARY";

    private final int charsetId, collationId;
    private final Map<TableName, Group> groups = new TreeMap<>();
    private final Map<TableName, Table> tables = new TreeMap<>();
    private final Map<TableName, Sequence> sequences = new TreeMap<>();
    private final Map<TableName, View> views = new TreeMap<>();
    private final Map<TableName, Routine> routines = new TreeMap<>();
    private final Map<TableName, SQLJJar> sqljJars = new TreeMap<>();
    private final Map<TableName, Constraint> constraints = new TreeMap<>();
    private final Map<String, Join> joins = new TreeMap<>();
    private final Map<String, Schema> schemas = new TreeMap<>();
    private final ConcurrentMap cachedValues = new ConcurrentHashMap(4,0.75f,4); // Very few, write-once entries expected
    private long generation = -1;

    private Map<Integer, Table> tablesById = null;
    private boolean isFrozen = false;

    private static class AISFailureList extends AISValidationResults implements AISValidationOutput {

        @Override
        public void reportFailure(AISValidationFailure failure) {
            if (failure != null) {
                failureList.add(failure);
            }
        }
        public AISFailureList() {
            failureList = new LinkedList<>();
        }
    }
}
