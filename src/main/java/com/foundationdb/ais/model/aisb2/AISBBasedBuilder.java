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

package com.foundationdb.ais.model.aisb2;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.ais.model.validation.AISValidationResults;
import com.foundationdb.ais.model.validation.AISValidations;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.InvalidSQLJJarURLException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

public class AISBBasedBuilder
{
    public static NewAISBuilder create(TypesTranslator typesTranslator) {
        return new ActualBuilder(typesTranslator);
    }

    public static NewAISBuilder create(String defaultSchema,
                                       TypesTranslator typesTranslator) {
        return new ActualBuilder(typesTranslator).defaultSchema(defaultSchema);
    }

    private static class ActualBuilder implements NewViewBuilder, NewAkibanJoinBuilder, NewRoutineBuilder, NewSQLJJarBuilder {

        // NewAISProvider interface

        @Override
        public AkibanInformationSchema ais() {
            return ais(true);
        }

        @Override
        public AkibanInformationSchema ais(boolean freezeAIS) {
            usable = false;
            aisb.basicSchemaIsComplete();
            aisb.groupingIsComplete();
            AISValidationResults results = aisb.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS);
            results.throwIfNecessary();
            if (freezeAIS) {
                aisb.akibanInformationSchema().freeze();
            }
            return aisb.akibanInformationSchema();
        }
        
        @Override
        public AkibanInformationSchema unvalidatedAIS() {
            aisb.basicSchemaIsComplete();
            aisb.groupingIsComplete();
            return aisb.akibanInformationSchema();
        }
        // NewAISBuilder interface

        @Override
        public NewAISBuilder defaultSchema(String schema) {
            this.defaultSchema = schema;
            return this;
        }

        @Override
        public NewTableBuilder table(String table) {
            return table(defaultSchema, table);
        }

        @Override
        public NewTableBuilder table(String schema, String table) {
            checkUsable();
            AISInvariants.checkDuplicateTables(aisb.akibanInformationSchema(), schema, table);
            this.schema = schema;
            this.object = table;
            TableName tableName= new TableName (schema, table);
            aisb.table(schema, table);
            aisb.createGroup(table, schema);
            aisb.addTableToGroup(tableName, schema, table);
            tablesToGroups.put(TableName.create(schema, table), tableName);
            tableColumnPos = 0;
            return this;
        }

        @Override
        public NewTableBuilder getTable() {
            return this;
        }
        
        @Override 
        public NewTableBuilder getTable(TableName table) {
            checkUsable();
            this.schema = table.getSchemaName();
            this.object = table.getTableName();
            assert aisb.akibanInformationSchema().getTable(table) != null;
            tableColumnPos = aisb.akibanInformationSchema().getTable(table).getColumns().size();
            return this;
        }
        
        @Override
        public NewAISBuilder sequence (String name) {
            return sequence (name, 1, 1, false);
        }
        
        @Override
        public NewAISBuilder sequence (String name, long start, long increment, boolean isCycle) {
            checkUsable();
            AISInvariants.checkDuplicateSequence(aisb.akibanInformationSchema(), defaultSchema, name);
            aisb.sequence(defaultSchema, name, start, increment, Long.MIN_VALUE, Long.MAX_VALUE, isCycle);
            return this;
        }

        @Override
        public NewTableBuilder table(TableName tableName) {
            return table(tableName.getSchemaName(), tableName.getTableName());
        }

        @Override
        public NewViewBuilder view(String view) {
            return view(defaultSchema, view);
        }

        @Override
        public NewViewBuilder view(String schema, String view) {
            checkUsable();
            AISInvariants.checkDuplicateTables(aisb.akibanInformationSchema(), schema, view);
            this.schema = schema;
            this.object = view;
            return this;
        }

        @Override
        public NewViewBuilder view(TableName viewName) {
            return view(viewName.getSchemaName(), viewName.getTableName());
        }

        @Override
        public NewRoutineBuilder procedure(String procedure) {
            return procedure(defaultSchema, procedure);
        }

        @Override
        public NewRoutineBuilder procedure(String schema, String procedure) {
            checkUsable();
            AISInvariants.checkDuplicateRoutine(aisb.akibanInformationSchema(), schema, procedure);
            this.schema = schema;
            this.object = procedure;
            return this;
        }

        @Override
        public NewRoutineBuilder procedure(TableName procedureName) {
            return procedure(procedureName.getSchemaName(), procedureName.getTableName());
        }

        @Override
        public NewSQLJJarBuilder sqljJar(String jarName) {
            return sqljJar(defaultSchema, jarName);
        }

        @Override
        public NewSQLJJarBuilder sqljJar(String schema, String jarName) {
            checkUsable();
            AISInvariants.checkDuplicateSQLJJar(aisb.akibanInformationSchema(), schema, jarName);
            this.schema = schema;
            this.object = jarName;
            return this;
        }

        @Override
        public NewSQLJJarBuilder sqljJar(TableName name) {
            return sqljJar(name.getSchemaName(), name.getTableName());
        }

        @Override
        public NewAISGroupIndexStarter groupIndex(String indexName, Index.JoinType joinType) {
            ActualGroupIndexBuilder actual  = new ActualGroupIndexBuilder(aisb, defaultSchema);
            if (joinType == null) {
                throw new InvalidParameterValueException("JoinType cannot be null");
            }
            return actual.groupIndex(indexName, joinType);
        }

        // NewTableBuilder interface

        @Override
        public NewTableBuilder colInt(String name) {
            return colLong(name, NULLABLE_DEFAULT, null, null);
        }

        @Override
        public NewTableBuilder colInt(String name, boolean nullable) {
            return colLong(name, nullable, null, null);
        }

        @Override
        public NewTableBuilder autoIncInt(String name, int initialValue) {
            return colLong(name, false, initialValue, true);
        }

        @Override
        public NewTableBuilder autoIncInt(String name, int initialValue, boolean always) {
            return colLong(name, false, initialValue, !always);
        }

        private NewTableBuilder colLong(String name, boolean nullable, Integer initialAutoInc, Boolean defaultIdentity) {
            checkUsable();
            TInstance type = typesTranslator.typeForJDBCType(Types.INTEGER, nullable,
                                                             schema, object, name);
            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            if (initialAutoInc != null) {
                assert defaultIdentity != null;
                String sequenceName = "temp-seq-" + object + "-" + name;
                long initValue = initialAutoInc.longValue();
                aisb.sequence(schema, sequenceName, 
                              initValue, 1L, initValue, Long.MAX_VALUE,
                              false);
                aisb.columnAsIdentity(schema, object, name, sequenceName, defaultIdentity);
                aisb.akibanInformationSchema().
                     getTable(schema, object).
                     getColumn(name).
                     setInitialAutoIncrementValue(initValue);
            }
            return this;
        }

        @Override
        public NewTableBuilder colBoolean(String name, boolean nullable) {
            checkUsable();
            TInstance type = typesTranslator.typeForJDBCType(Types.BOOLEAN, nullable,
                                                             schema, object, name);
            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            return this;
        }

        @Override
        public NewTableBuilder colString(String name, int length) {
            return colString(name, length, NULLABLE_DEFAULT);
        }

        @Override
        public NewTableBuilder colString(String name, int length, boolean nullable) {
            return colString(name, length, nullable, CHARSET_DEFAULT);
        }

        @Override
        public NewTableBuilder colString(String name, int length, boolean nullable, String charset) {
            checkUsable();
            Table table = aisb.akibanInformationSchema().getTable(schema, object);
            TInstance type = typesTranslator.typeForString(length, charset, null, table.getDefaultedCharsetId(), table.getDefaultedCollationId(), nullable);
            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            return this;
        }

        @Override
        public NewTableBuilder colDouble(String name) {
            return colDouble(name, NULLABLE_DEFAULT);
        }

        @Override
        public NewTableBuilder colDouble(String name, boolean nullable) {
            checkUsable();
            TInstance type = typesTranslator.typeForJDBCType(Types.DOUBLE, nullable,
                                                             schema, object, name);
            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            return this;
        }

        @Override
        public NewTableBuilder colBigInt(String name) {
            return colBigInt(name, NULLABLE_DEFAULT);
        }

        @Override
        public NewTableBuilder colBigInt(String name, boolean nullable) {
            TInstance type = typesTranslator.typeForJDBCType(Types.BIGINT, nullable,
                                                             schema, object, name);
            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            return this;
        }

        @Override
        public NewTableBuilder colVarBinary(String name, int length) {
            return colVarBinary(name, length, NULLABLE_DEFAULT);
        }

        @Override
        public NewTableBuilder colVarBinary(String name, int length, boolean nullable) {
            TInstance type = typesTranslator.typeForJDBCType(Types.VARBINARY, length, nullable,
                                                             schema, object, name);

            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            return this;
        }

        @Override
        public NewTableBuilder colText(String name) {
            return colText(name, NULLABLE_DEFAULT);
        }

        @Override
        public NewTableBuilder colText(String name, boolean nullable) {
            TInstance type = typesTranslator.typeForJDBCType(Types.LONGVARCHAR, nullable,
                                                             schema, object, name);
            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            return this;
        }

        /*
        @Override
        public NewTableBuilder colTimestamp(String name) {
            return colTimestamp(name, NULLABLE_DEFAULT);
        }

        @Override
        public NewTableBuilder colTimestamp(String name, boolean nullable) {
            TInstance type = typesTranslator.typeForJDBCType(Types.TIMESTAMP, nullable,
                                                             schema, object, name);
            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            return this;
        }
        */

        @Override
        public NewTableBuilder colSystemTimestamp(String name) {
            return colSystemTimestamp(name, NULLABLE_DEFAULT);
        }

        @Override
        public NewTableBuilder colSystemTimestamp(String name, boolean nullable) {
            TInstance type = typesTranslator.typeClassForSystemTimestamp().instance(nullable);
            aisb.column(schema, object, name, tableColumnPos++, type, false, null, null);
            return this;
        }

        @Override
        public NewTableBuilder pk(String... columns) {
            aisb.pk(schema, object);
            return columns(Index.PRIMARY, columns);
        }

        @Override
        public NewTableBuilder uniqueKey(String indexName, String... columns) {
            aisb.unique(schema, object, indexName);
            return columns(indexName, columns);
        }

        @Override
        public NewTableBuilder uniqueConstraint(String constraintName, String indexName, String... columns) {
            aisb.uniqueConstraint(schema, object, indexName, new TableName(schema, constraintName));
            return columns(indexName, columns);
        }

        @Override
        public NewTableBuilder key(String indexName, String... columns) {
            aisb.index(schema, object, indexName);
            return columns(indexName, columns);
        }

        private NewTableBuilder columns(String indexName, String[] columns) {
            checkUsable();
            for (int i=0; i < columns.length; ++i) {
                aisb.indexColumn(schema, object, indexName, columns[i], i, true, null);
            }
            return this;
        }

        @Override
        public NewAkibanJoinBuilder joinTo(String table) {
            return joinTo(schema, table);
        }

        @Override
        public NewAkibanJoinBuilder joinTo(TableName name) {
            return joinTo(name.getSchemaName(), name.getTableName());
        }

        @Override
        public NewAkibanJoinBuilder joinTo(String schema, String table) {
            String generated = "autogenerated_"+object+"_references_" + table;
            return joinTo(schema, table, generated);
        }

        @Override
        public NewAkibanJoinBuilder joinTo(String schema, String table, String fkName) {
            checkUsable();
            this.fkIndexName = fkName;
            this.fkJoinName = fkIndexName;
            this.fkIndexPos = 0;
            this.referencesSchema = schema;
            this.referencesTable = table;

            Group oldGroup = aisb.akibanInformationSchema().getTable(this.schema, this.object).getGroup();

            aisb.index(this.schema, this.object, fkIndexName);
            aisb.joinTables(fkJoinName, schema, table, this.schema, this.object);

            TableName fkGroupName = tablesToGroups.get(TableName.create(referencesSchema, referencesTable));
            aisb.moveTreeToGroup(this.schema, this.object, fkGroupName, fkJoinName);
            aisb.akibanInformationSchema().removeGroup(oldGroup);
            TableName oldGroupName = tablesToGroups.put(TableName.create(this.schema, this.object), fkGroupName);
            assert oldGroup.getName().equals(oldGroupName) : oldGroup.getName() + " != " + oldGroupName;
            return this;
        } 

        // NewAkibanJoinBuilder

        @Override
        public NewAkibanJoinBuilder on(String childColumn, String parentColumn) {
            checkUsable();
            aisb.indexColumn(schema, object, fkIndexName, childColumn, fkIndexPos, true, null);
            aisb.joinColumns(fkJoinName, referencesSchema, referencesTable, parentColumn, schema, object, childColumn);
            return this;
        }

        @Override
        public NewAkibanJoinBuilder and(String childColumn, String parentColumn) {
            return on(childColumn, parentColumn);
        }

        // NewViewBuilder

        @Override
        public NewViewBuilder definition(String definition) {
            Properties properties = new Properties();
            properties.put("database", schema);
            return definition(definition, properties);
        }

        @Override
        public NewViewBuilder definition(String definition, Properties properties) {
            aisb.view(schema, object,
                      definition, properties, 
                      new HashMap<TableName,Collection<String>>());
            return this;
        }

        @Override
        public NewViewBuilder references(String table) {
            return references(schema, table);
        }

        @Override
        public NewViewBuilder references(String schema, String table, String... columns) {
            checkUsable();
            View view = aisb.akibanInformationSchema().getView(this.schema, this.object);
            TableName tableName = TableName.create(schema, table);
            Collection<String> entry = view.getTableColumnReferences().get(tableName);
            if (entry == null) {
                entry = new HashSet<>();
                view.getTableColumnReferences().put(tableName, entry);
            }
            for (String colname : columns) {
                entry.add(colname);
            }
            return this;
        }

        // NewRoutineBuilder

        @Override
        public NewRoutineBuilder language(String language, Routine.CallingConvention callingConvention) {
            aisb.routine(schema, object, language, callingConvention);
            return this;
        }

        @Override
        public NewRoutineBuilder returnBoolean(String name) {
            TInstance type = typesTranslator.typeForJDBCType(Types.BOOLEAN, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.RETURN, type);
            return this;
        }

        @Override
        public NewRoutineBuilder returnLong(String name) {
            TInstance type = typesTranslator.typeForJDBCType(Types.BIGINT, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.RETURN, type);
            return this;
        }

        @Override
        public NewRoutineBuilder returnString(String name, int length) {
            TInstance type = typesTranslator.typeForJDBCType(Types.VARCHAR, length, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.RETURN, type);
            return this;
        }

        @Override
        public NewRoutineBuilder paramBooleanIn(String name) {
            TInstance type = typesTranslator.typeForJDBCType(Types.BOOLEAN, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.IN, type);
            return this;
        }

        @Override
        public NewRoutineBuilder paramLongIn(String name) {
            TInstance type = typesTranslator.typeForJDBCType(Types.BIGINT, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.IN, type);
            return this;
        }

        @Override
        public NewRoutineBuilder paramStringIn(String name, int length) {
            TInstance type = typesTranslator.typeForJDBCType(Types.VARCHAR, length, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.IN, type);
            return this;
        }

        @Override
        public NewRoutineBuilder paramDoubleIn(String name) {
            TInstance type = typesTranslator.typeForJDBCType(Types.DOUBLE, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.IN, type);
            return this;
        }

        @Override
        public NewRoutineBuilder paramLongOut(String name) {
            TInstance type = typesTranslator.typeForJDBCType(Types.BIGINT, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.OUT, type);
            return this;
        }

        @Override
        public NewRoutineBuilder paramStringOut(String name, int length) {
            TInstance type = typesTranslator.typeForJDBCType(Types.VARCHAR, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.OUT, type);
            return this;
        }

        @Override
        public NewRoutineBuilder paramDoubleOut(String name) {
            TInstance type = typesTranslator.typeForJDBCType(Types.DOUBLE, true,
                                                             schema, object, name);
            aisb.parameter(schema, object, name, Parameter.Direction.OUT, type);
            return this;
        }

        @Override
        public NewRoutineBuilder externalName(String className) {
            return externalName(className, null);
        }

        @Override
        public NewRoutineBuilder externalName(String className, String methodName) {
            return externalName(null, className, methodName);
        }

        @Override
        public NewRoutineBuilder externalName(String jarName,
                                              String className, String methodName) {
            return externalName(defaultSchema, jarName, className, methodName);
        }

        @Override
        public NewRoutineBuilder externalName(String jarSchema, String jarName, 
                                              String className, String methodName) {
            aisb.routineExternalName(schema, object, 
                                     jarSchema, jarName, 
                                     className, methodName);
            return this;
        }

        @Override
        public NewRoutineBuilder procDef(String definition) {
            aisb.routineDefinition(schema, object, definition);
            return this;
        }

        @Override
        public NewRoutineBuilder sqlAllowed(Routine.SQLAllowed sqlAllowed) {
            aisb.routineSQLAllowed(schema, object, sqlAllowed);
            return this;
        }

        @Override
        public NewRoutineBuilder dynamicResultSets(int dynamicResultSets) {
            aisb.routineDynamicResultSets(schema, object, dynamicResultSets);
            return this;
        }

        @Override
        public NewRoutineBuilder deterministic(boolean deterministic) {
            aisb.routineDeterministic(schema, object, deterministic);
            return this;
        }

        @Override
        public NewRoutineBuilder calledOnNullInput(boolean calledOnNullInput) {
            aisb.routineCalledOnNullInput(schema, object, calledOnNullInput);
            return this;
        }

        // NewSQLJJarBuilder

        @Override
        public NewSQLJJarBuilder url(String value, boolean checkReadable) {
            URL url;
            try {
                url = new URL(value);
            }
            catch (MalformedURLException ex1) {
                File file = new File(value);
                try {
                    url = file.toURI().toURL();
                }
                catch (MalformedURLException ex2) {
                    // Report original failure.
                    throw new InvalidSQLJJarURLException(schema, object, ex1);
                }
                if (checkReadable && file.canRead())
                    checkReadable = false; // Can tell quickly.
            }
            if (checkReadable) {
                InputStream istr = null;
                try {
                    istr = url.openStream(); // Must be able to load it.
                }
                catch (IOException ex) {
                    throw new InvalidSQLJJarURLException(schema, object, ex);
                }
                finally {
                    if (istr != null) {
                        try {
                            istr.close();
                        }
                        catch (IOException ex) {
                        }
                    }
                }
            }
            aisb.sqljJar(schema, object, url);
            return this;
        }

        // ActualBuilder interface

        public ActualBuilder(TypesTranslator typesTranslator) {
            this.aisb = new AISBuilder();
            this.typesTranslator = typesTranslator;
            usable = true;
            tablesToGroups = new HashMap<>();
        }

        // private

        private void checkUsable() {
            if (!usable) {
                throw new IllegalStateException("AIS has already been retrieved; can't reuse");
            }
        }

        // object state

        private final AISBuilder aisb;
        private final TypesTranslator typesTranslator;
        private String defaultSchema;
        private String schema;
        private String object;

        private int tableColumnPos;

        private String fkIndexName;
        private String fkJoinName;
        private int fkIndexPos;
        private String referencesSchema;
        private String referencesTable;

        private boolean usable;

        private final Map<TableName,TableName> tablesToGroups;
        // constants

        private static final boolean NULLABLE_DEFAULT = false;
        private static final String CHARSET_DEFAULT = "UTF-8";
        private static final String PRIMARY = "PRIMARY";
    }

    private static class ActualGroupIndexBuilder implements NewAISGroupIndexStarter, NewAISGroupIndexBuilder {

        // NewAISProvider interface

        @Override
        public AkibanInformationSchema ais(boolean freezeAIS) {
            return ais();
        }

        @Override
        public AkibanInformationSchema ais() {
            if (unstartedIndex()) {
                throw new IllegalStateException("a groupIndex was started but not given any columns: " + indexName);
            }
            return aisb.akibanInformationSchema();
        }

        @Override
        public AkibanInformationSchema unvalidatedAIS() {
            return aisb.akibanInformationSchema();
        }
        // NewAISGroupIndexBuilder interface


        @Override
        public NewAISGroupIndexStarter groupIndex(String indexName, Index.JoinType joinType) {
            this.indexName = indexName;
            this.groupName = null;
            this.position = -1;
            this.joinType = joinType;
            return this;
        }

        @Override
        public NewAISGroupIndexBuilder on(String table, String column) {
            return on(defaultSchema, table, column);
        }

        @Override
        public NewAISGroupIndexBuilder on(String schema, String table, String column) {
            Table aisTable = aisb.akibanInformationSchema().getTable(schema, table);
            if (aisTable == null) {
                throw new NoSuchElementException("no table " + schema + '.' + table);
            }
            if (aisTable.getGroup() == null) {
                throw new IllegalStateException("ungrouped table: " + schema + '.' + table);
            }
            TableName localGroupName = aisTable.getGroup().getName();
            if (localGroupName == null) {
                throw new IllegalStateException("unnamed group for " + schema + '.' + table);
            }
            this.groupName = localGroupName;
            this.position = 0;
            aisb.groupIndex(this.groupName, this.indexName, false, joinType);
            return and(schema, table, column);
        }

        @Override
        public NewAISGroupIndexBuilder and(String table, String column) {
            return and(defaultSchema, table, column);
        }

        @Override
        public NewAISGroupIndexBuilder and(String schema, String table, String column) {
            if (unstartedIndex()) {
                throw new IllegalStateException("never called on(table,column) for " + indexName);
            }
            aisb.groupIndexColumn(groupName, indexName, schema, table, column, position++);
            return this;
        }

        // ActualFinisher interface

        public ActualGroupIndexBuilder(AISBuilder aisb, String defaultSchema) {
            this.aisb = aisb;
            this.defaultSchema = defaultSchema;
        }

        // private methods

        private boolean unstartedIndex() {
            // indexName is assigned as soon as groupIndex is invoked, but groupName is only resolved
            // by on.
            boolean hasUnstarted = (indexName != null) && (groupName == null);
            assert hasUnstarted == (position < 0) : String.format("%s but %d", hasUnstarted, position);
            return hasUnstarted;
        }

        // object states

        private final AISBuilder aisb;
        private final String defaultSchema;
        private int position;
        private Index.JoinType joinType;
        private String indexName;
        private TableName groupName;
    }
}
