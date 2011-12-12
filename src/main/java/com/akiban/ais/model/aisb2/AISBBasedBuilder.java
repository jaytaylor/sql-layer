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

package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.validation.AISInvariants;
import com.akiban.ais.model.validation.AISValidationResults;
import com.akiban.ais.model.validation.AISValidations;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class AISBBasedBuilder
{
    public static NewAISBuilder create() {
        return new ActualBuilder();
    }

    public static NewAISBuilder create(String defaultSchema) {
        return new ActualBuilder().defaultSchema(defaultSchema);
    }

    private static class ActualBuilder implements NewAISBuilder, NewUserTableBuilder, NewAkibanJoinBuilder {

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
            AISValidationResults results = aisb.akibanInformationSchema().validate(AISValidations.LIVE_AIS_VALIDATIONS);
            results.throwIfNecessary();
            if (freezeAIS) {
                aisb.akibanInformationSchema().freeze();
            }
            return aisb.akibanInformationSchema();
        }

        // NewAISBuilder interface

        @Override
        public NewAISBuilder defaultSchema(String schema) {
            this.defaultSchema = schema;
            return this;
        }

        @Override
        public NewUserTableBuilder userTable(String table) {
            return userTable(defaultSchema, table);
        }

        @Override
        public NewUserTableBuilder userTable(String schema, String table) {
            checkUsable();
            AISInvariants.checkDuplicateTables(aisb.akibanInformationSchema(), schema, table);
            this.schema = schema;
            this.userTable = table;
            TableName tableName= new TableName (schema, table);
            aisb.userTable(schema, table);
            String groupName = nameGenerator.generateGroupName(aisb.akibanInformationSchema().getUserTable(tableName));
            aisb.createGroup(groupName, schema, nameGenerator.generateGroupTableName(groupName));
            aisb.addTableToGroup(groupName, schema, table);
            tablesToGroups.put(TableName.create(schema, table), groupName);
            uTableColumnPos = 0;
            return this;
        }

        @Override
        public NewAISGroupIndexStarter groupIndex(String indexName) {
            return groupIndex(indexName, null);
        }

        @Override
        public NewAISGroupIndexStarter groupIndex(String indexName, Index.JoinType joinType) {
            return new ActualGroupIndexBuilder(aisb.akibanInformationSchema(), defaultSchema).groupIndex(indexName, joinType);
        }

        // NewuserTableBuilder interface

        @Override
        public NewUserTableBuilder colLong(String name) {
            return colLong(name, false, NULLABLE_DEFAULT);
        }

        @Override
        public NewUserTableBuilder colLong(String name, boolean nullable) {
            return colLong(name, nullable, false);
        }

        @Override
        public NewUserTableBuilder autoIncLong(String name) {
            return colLong(name, false, true);
        }

        private NewUserTableBuilder colLong(String name, boolean nullable, boolean autoIncrement) {
            checkUsable();
            aisb.column(schema, userTable, name, uTableColumnPos++, "INT", 10L, null, nullable, autoIncrement, null, null);
            return this;
        }

        @Override
        public NewUserTableBuilder colString(String name, int length) {
            return colString(name, length, NULLABLE_DEFAULT);
        }

        @Override
        public NewUserTableBuilder colString(String name, int length, boolean nullable) {
            return colString(name, length, nullable, CHARSET_DEFAULT);
        }

        @Override
        public NewUserTableBuilder colString(String name, int length, boolean nullable, String charset) {
            checkUsable();
            aisb.column(schema, userTable, name, uTableColumnPos++, "VARCHAR", (long)length, null, nullable, false, charset, null);
            return this;
        }

        @Override
        public NewUserTableBuilder colDouble(String name) {
            return colDouble(name, NULLABLE_DEFAULT);
        }

        @Override
        public NewUserTableBuilder colDouble(String name, boolean nullable) {
            checkUsable();
            aisb.column(schema, userTable, name, uTableColumnPos++, "DOUBLE", null, null, nullable, false, null, null);
            return this;
        }

        @Override
        public NewUserTableBuilder pk(String... columns) {
            return key(PRIMARY, columns, true, Index.PRIMARY_KEY_CONSTRAINT);
        }

        @Override
        public NewUserTableBuilder uniqueKey(String indexName, String... columns) {
            return key(indexName, columns, true, Index.UNIQUE_KEY_CONSTRAINT);
        }

        @Override
        public NewUserTableBuilder key(String indexName, String... columns) {
            return key(indexName, columns, false, Index.KEY_CONSTRAINT);
        }

        private NewUserTableBuilder key(String indexName, String[] columns, boolean unique, String constraint) {
            checkUsable();
            aisb.index(schema, userTable, indexName, unique, constraint);
            for (int i=0; i < columns.length; ++i) {
                aisb.indexColumn(schema, userTable, indexName, columns[i], i, false, null);
            }
            return this;
        }

        @Override
        public NewAkibanJoinBuilder joinTo(String table) {
            return joinTo(schema, table);
        }

        @Override
        public NewAkibanJoinBuilder joinTo(String schema, String table) {
            String generated = "autogenerated_"+userTable+"_references_" + table;
            return joinTo(schema, table, generated);
        }

        @Override
        public NewAkibanJoinBuilder joinTo(String schema, String table, String fkName) {
            checkUsable();
            this.fkIndexName = "__akiban_" + fkName;
            this.fkJoinName = "join_" + fkIndexName;
            this.fkIndexPos = 0;
            this.referencesSchema = schema;
            this.referencesTable = table;

            Group oldGroup = aisb.akibanInformationSchema().getUserTable(this.schema, this.userTable).getGroup();

            aisb.index(this.schema, this.userTable, fkIndexName, false, Index.KEY_CONSTRAINT);
            aisb.joinTables(fkJoinName, schema, table, this.schema, this.userTable);

            String fkGroupName = tablesToGroups.get(TableName.create(referencesSchema, referencesTable));
            aisb.moveTreeToGroup(this.schema, this.userTable, fkGroupName, fkJoinName);
            aisb.akibanInformationSchema().removeGroup(oldGroup);
            String oldGroupName = tablesToGroups.put(TableName.create(this.schema, this.userTable), fkGroupName);
            assert oldGroup.getName().equals(oldGroupName) : oldGroup.getName() + " != " + oldGroupName;
            return this;
        }

        // NewAkibanJoinBuilder

        @Override
        public NewAkibanJoinBuilder on(String childColumn, String parentColumn) {
            checkUsable();
            aisb.indexColumn(schema, userTable, fkIndexName, childColumn, fkIndexPos, false, null);
            aisb.joinColumns(fkJoinName, referencesSchema, referencesTable, parentColumn, schema, userTable, childColumn);
            return this;
        }

        @Override
        public NewAkibanJoinBuilder and(String childColumn, String parentColumn) {
            return on(childColumn, parentColumn);
        }

        // ActualBuilder interface

        public ActualBuilder() {
            aisb = new AISBuilder();
            usable = true;
            tablesToGroups = new HashMap<TableName, String>();
            nameGenerator = new DefaultNameGenerator();
        }

        // private

        private void checkUsable() {
            if (!usable) {
                throw new IllegalStateException("AIS has already been retrieved; can't reuse");
            }
        }

        // object state

        private final AISBuilder aisb;
        private String defaultSchema;
        private String schema;
        private String userTable;

        private int uTableColumnPos;

        private String fkIndexName;
        private String fkJoinName;
        private int fkIndexPos;
        private String referencesSchema;
        private String referencesTable;

        private boolean usable;

        private final Map<TableName,String> tablesToGroups;
        private final NameGenerator nameGenerator;
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

        // NewAISGroupIndexBuilder interface

        @Override
        public NewAISGroupIndexStarter groupIndex(String indexName) {
            return groupIndex(indexName, null);
        }

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
            UserTable userTable = aisb.akibanInformationSchema().getUserTable(schema, table);
            if (userTable == null) {
                throw new NoSuchElementException("no table " + schema + '.' + table);
            }
            if (userTable.getGroup() == null) {
                throw new IllegalStateException("ungrouped table: " + schema + '.' + table);
            }
            String localGroupName = userTable.getGroup().getName();
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

        public ActualGroupIndexBuilder(AkibanInformationSchema ais, String defaultSchema) {
            this.aisb = new AISBuilder(ais);
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
        private String groupName;
    }
}
