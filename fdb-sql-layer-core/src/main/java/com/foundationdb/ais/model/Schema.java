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

import java.util.Map;
import java.util.TreeMap;

public class Schema {
    public static Schema create(AkibanInformationSchema ais, String schemaName) {
        ais.checkMutability();
        Schema schema = new Schema(schemaName);
        ais.addSchema(schema);
        return schema;
    }

    public String getName() {
        return name;
    }

    public Map<String, Table> getTables() {
        return tables;
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    void addTable(Table table) {
        tables.put(table.getName().getTableName(), table);
    }

    void removeTable(String tableName) {
        tables.remove(tableName);
    }

    public Map<String, Sequence> getSequences() {
        return sequences;
    }
    
    public Sequence getSequence (String sequenceName) {
        return sequences.get(sequenceName);
    }
    
    void addSequence (Sequence sequence) {
        sequences.put(sequence.getSequenceName().getTableName(), sequence);
    }
    
    void removeSequence (String sequenceName) {
        sequences.remove(sequenceName);
    }
    
    void addConstraint (Constraint constraint) {
        constraints.put(constraint.getConstraintName().getTableName(), constraint);
    }
    
    public boolean hasConstraint(String constraintName){
        return constraints.containsKey(constraintName);
    }
    
    public Constraint getConstraint(String constraintName) {return constraints.get(constraintName); }
    
    void removeConstraint (String constraintName) {
        constraints.remove(constraintName);
    }
    
    public Map<String, View> getViews() {
        return views;
    }

    public View getView(String viewName) {
        return views.get(viewName);
    }

    void addView(View view) {
        views.put(view.getName().getTableName(), view);
    }

    void removeView(String viewName) {
        views.remove(viewName);
    }

    public Map<String, Routine> getRoutines() {
        return routines;
    }
    
    public Routine getRoutine(String routineName) {
        return routines.get(routineName);
    }
    
    void addRoutine(Routine routine) {
        routines.put(routine.getName().getTableName(), routine);
    }
    
    void removeRoutine(String routineName) {
        routines.remove(routineName);
    }
    
    public Map<String, SQLJJar> getSQLJJars() {
        return sqljJars;
    }
    
    public SQLJJar getSQLJJar(String sqljJarName) {
        return sqljJars.get(sqljJarName);
    }
    
    void addSQLJJar(SQLJJar sqljJar) {
        sqljJars.put(sqljJar.getName().getTableName(), sqljJar);
    }
    
    void removeSQLJJar(String sqljJarName) {
        sqljJars.remove(sqljJarName);
    }
    
    @Override
    public String toString() {
        return name;
    }

    Schema(String name) {
        this.name = name;
    }

    private final String name;
    private final Map<String, Table> tables = new TreeMap<>();
    private final Map<String, Sequence> sequences = new TreeMap<>();
    private final Map<String, View> views = new TreeMap<>();
    private final Map<String, Routine> routines = new TreeMap<>();
    private final Map<String, SQLJJar> sqljJars = new TreeMap<>();
    private final Map<String, Constraint> constraints = new TreeMap<>();
}
