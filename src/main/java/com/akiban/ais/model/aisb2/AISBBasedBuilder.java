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
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AISBBasedBuilder
{
    public static NewAISBuilder create() {
        return new ActualBuilder();
    }

    public static NewAISBuilder create(String defaultSchema) {
        return new ActualBuilder().defaultSchema(defaultSchema);
    }

    private static class ActualBuilder implements NewAISBuilder, NewUserTableBuilder, NewAkibanJoinBuilder {

        // ActualBuilder interface

        @Override
        public AkibanInformationSchema ais() {
            usable = false;
            aisb.basicSchemaIsComplete();
            aisb.groupingIsComplete();
            return aisb.akibanInformationSchema();
        }

        @Override
        public NewAISBuilder defaultSchema(String schema) {
            this.schema = schema;
            return this;
        }

        @Override
        public NewUserTableBuilder userTable(String table) {
            return userTable(schema, table);
        }

        @Override
        public NewUserTableBuilder userTable(String schema, String table) {
            checkUsable();
            if (aisb.akibanInformationSchema().getUserTable(schema, table) != null) {
                throw new IllegalArgumentException("table " + schema + '.' + table + " already defined");
            }
            String groupName = groupName(table);
            this.schema = schema;
            this.userTable = table;
            aisb.userTable(schema, table);
            aisb.createGroup(groupName, schema, "_akiban_"+schema+'_'+table);
            aisb.addTableToGroup(groupName, schema, table);
            tablesPerGroup.put(groupName, 1);
            tablesToGroups.put(TableName.create(schema, table), groupName);
            uTableColumnPos = 0;
            return this;
        }

        private String groupName(String table) {
            int n = 0;
            String groupName = table;
            while (groupNames.contains(groupName)) {
                groupName = table + '$' + n;
            }
            return groupName;
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
            aisb.column(schema, userTable, name, ++uTableColumnPos, "INT", 10L, null, nullable, autoIncrement, null, null);
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
            aisb.column(schema, userTable, name, ++uTableColumnPos, "VARCHAR", (long)length, null, nullable, false, charset, null);
            return this;
        }

        @Override
        public NewUserTableBuilder pk(String... columns) {
            return key(PRIMARY, columns, true, Index.PRIMARY_KEY_CONSTRAINT);
        }

        @Override
        public NewUserTableBuilder uniqueKey(String indexName, String... columns) {
            return key(indexName, columns, true, "UNIQUE KEY");
        }

        @Override
        public NewUserTableBuilder key(String indexName, String... columns) {
            return key(indexName, columns, false, "KEY");
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
            aisb.index(this.schema, this.userTable, fkIndexName, false, "KEY");
            aisb.joinTables(fkJoinName, schema, table, this.schema, this.userTable);

            String fkGroupName = tablesToGroups.get(TableName.create(referencesSchema, referencesTable));
            aisb.moveTreeToGroup(this.schema, this.userTable, fkGroupName, fkJoinName);
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
            groupNames = new HashSet<String>();
            tablesPerGroup = new HashMap<String, Integer>();
            tablesToGroups = new HashMap<TableName, String>();
        }

        // private

        private void checkUsable() {
            if (!usable) {
                throw new IllegalStateException("AIS has already been retrieved; can't reuse");
            }
        }

        // object state

        private final AISBuilder aisb;
        private String schema;
        private String userTable;

        private int uTableColumnPos;

        private String fkIndexName;
        private String fkJoinName;
        private int fkIndexPos;
        private String referencesSchema;
        private String referencesTable;

        private boolean usable;

        private final Set<String> groupNames;
        private final Map<String,Integer> tablesPerGroup;
        private final Map<TableName,String> tablesToGroups;

        // constants

        private static final boolean NULLABLE_DEFAULT = false;
        private static final String CHARSET_DEFAULT = "UTF-8";
        private static final String PRIMARY = "PRIMARY";
    }
}
