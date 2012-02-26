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

package com.akiban.server.rowdata;

import com.akiban.ais.metamodel.io.AISTarget;
import com.akiban.ais.metamodel.io.Writer;
import com.akiban.ais.model.AISMerge;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.MemoryOnlyTableStatusCache;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStoreSchemaManager;
import com.akiban.sql.aisddl.IndexDDL;
import com.akiban.sql.aisddl.TableDDL;
import com.akiban.sql.parser.CreateIndexNode;
import com.akiban.sql.parser.CreateTableNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import com.persistit.exception.PersistitInterruptedException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaFactory {
    private final static String DEFAULT_DEFAULT_SCHEMA = "test";
    private final String defaultSchema;


    public SchemaFactory() {
        this(DEFAULT_DEFAULT_SCHEMA);
    }
    
    public SchemaFactory(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public RowDefCache rowDefCache(String... ddl) throws Exception {
        AkibanInformationSchema ais = ais(ddl);
        return rowDefCache(ais);
    }

    public AkibanInformationSchema ais(String... ddl) throws Exception {
        StringBuilder buffer = new StringBuilder();
        for (String line : ddl) {
            buffer.append(line);
        }
        String fullDDL = buffer.toString();
        /*
        fullDDL = fullDDL.replaceAll("(?i)CONSTRAINT.*FOREIGN KEY.*?\\(", "GROUPING FOREIGN KEY (");
        fullDDL = fullDDL.replace('`', '"');
        fullDDL = fullDDL.replace("auto_increment", "");
        fullDDL = fullDDL.replaceAll("engine=akibandb", "");
        fullDDL = fullDDL.replaceAll(",\\s*KEY\\(.*\\)", "");
        fullDDL = fullDDL.replaceAll("\\s*;", ";");
        fullDDL = fullDDL.replaceAll(";\\s*", ";");
        fullDDL = fullDDL.replaceAll("(?i)id int", "id int not null");
        fullDDL = fullDDL.replaceAll("(?i)not null not null", "not null");
        fullDDL = fullDDL.trim();
        */
        CreateOnlyDDLMock ddlFunctions = new CreateOnlyDDLMock();
        SQLParser parser = new SQLParser();
        List<StatementNode> nodes;
        try {
            nodes = parser.parseStatements(fullDDL);
            for(StatementNode stmt : nodes) {
                if (stmt instanceof CreateTableNode) {
                    TableDDL.createTable(ddlFunctions , null, defaultSchema, (CreateTableNode) stmt);
                } else if (stmt instanceof CreateIndexNode) {
                    IndexDDL.createIndex(ddlFunctions, null, defaultSchema, (CreateIndexNode) stmt);
                } else {
                    throw new IllegalStateException("Unsupported StatementNode type: " + stmt);
                }
            }
        } catch(Exception e) {
            throw e;
        }
        return ddlFunctions.getAIS(null);
    }

    public RowDefCache rowDefCache(AkibanInformationSchema ais) {
        RowDefCache rowDefCache = new FakeRowDefCache();
        try {
            rowDefCache.setAIS(ais);
        } catch(PersistitInterruptedException e) {
            throw new PersistitAdapterException(e);
        }
        return rowDefCache;
    }

    private static class FakeRowDefCache extends RowDefCache {
        public FakeRowDefCache() {
            super(new MemoryOnlyTableStatusCache());
        }

        @Override
        protected Map<Table,Integer> fixUpOrdinals() throws PersistitInterruptedException {
            Map<Table,Integer> ordinalMap = new HashMap<Table,Integer>();
            for (RowDef groupRowDef : getRowDefs()) {
                if (groupRowDef.isGroupTable()) {
                    ordinalMap.put(groupRowDef.table(), 0);
                    int userTableOrdinal = 1;
                    for (RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
                        int ordinal = userTableOrdinal++;
                        tableStatusCache.setOrdinal(userRowDef.getRowDefId(), ordinal);
                        userRowDef.setOrdinalCache(ordinal);
                        ordinalMap.put(userRowDef.table(), ordinal);
                    }
                }
            }
            return ordinalMap;
        }
    }

    private static class CreateOnlyDDLMock implements DDLFunctions {
        AkibanInformationSchema ais = new AkibanInformationSchema();

        @Override
        public void createTable(Session session, UserTable newTable) {
            AISMerge merge = new AISMerge (ais, newTable);
            merge.merge();
            ais = merge.getAIS();
            final String schemaName = newTable.getName().getSchemaName();
            final UserTable finalTable = merge.getAIS().getUserTable(newTable.getName());
            //validateIndexSizes(newTable);
            //setTreeNames(finalTable);
        }

        @Override
        public void renameTable(Session session, TableName currentName, TableName newName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropTable(Session session, TableName tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropSchema(Session session, String schemaName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropGroup(Session session, String groupName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AkibanInformationSchema getAIS(Session session) {
            return ais;
        }

        @Override
        public TableName getTableName(Session session, int tableId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTableId(Session session, TableName tableName) throws NoSuchTableException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table getTable(Session session, int tableId) throws NoSuchTableIdException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table getTable(Session session, TableName tableName) throws NoSuchTableException {
            throw new UnsupportedOperationException();
        }

        @Override
        public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowDef getRowDef(int tableId) throws RowDefNotFoundException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getDDLs(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getGeneration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forceGenerationUpdate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
            AkibanInformationSchema newAIS = new AkibanInformationSchema();
            new Writer(new AISTarget(newAIS)).save(ais);
            PersistitStoreSchemaManager.createIndexes(newAIS, indexesToAdd);
            ais = newAIS;
        }

        @Override
        public void dropTableIndexes(Session session, TableName tableName, Collection<String> indexesToDrop) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropGroupIndexes(Session session, String groupName, Collection<String> indexesToDrop) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate) {
            throw new UnsupportedOperationException();
        }
    }
}