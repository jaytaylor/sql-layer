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

package com.akiban.server.rowdata;

import com.akiban.ais.metamodel.io.AISTarget;
import com.akiban.ais.metamodel.io.Writer;
import com.akiban.ais.model.AISMerge;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.server.MemoryOnlyTableStatusCache;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.service.dxl.IndexCheckSummary;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStoreSchemaManager;
import com.akiban.sql.StandardException;
import com.akiban.sql.aisddl.IndexDDL;
import com.akiban.sql.aisddl.TableDDL;
import com.akiban.sql.aisddl.ViewDDL;
import com.akiban.sql.optimizer.AISBinderContext;
import com.akiban.sql.parser.CreateIndexNode;
import com.akiban.sql.parser.CreateTableNode;
import com.akiban.sql.parser.CreateViewNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import com.akiban.util.Strings;
import com.persistit.exception.PersistitInterruptedException;

import java.io.File;
import java.io.IOException;
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

    public AkibanInformationSchema ais(String... ddl) {
        return ais(new AkibanInformationSchema(), ddl);
    }
    
    public static AkibanInformationSchema loadAIS(File fromFile, String defaultSchema) {
        try {
            List<String> ddl = Strings.dumpFile(fromFile);
            SchemaFactory schemaFactory = new SchemaFactory(defaultSchema);
            return schemaFactory.ais(ddl.toArray(new String[ddl.size()]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public AkibanInformationSchema ais(AkibanInformationSchema baseAIS, String... ddl) {
        StringBuilder buffer = new StringBuilder();
        for (String line : ddl) {
            buffer.append(line);
        }
        String fullDDL = buffer.toString();
        CreateOnlyDDLMock ddlFunctions = new CreateOnlyDDLMock(baseAIS);
        SQLParser parser = new SQLParser();
        List<StatementNode> nodes;
        try {
            nodes = parser.parseStatements(fullDDL);
        } catch(StandardException e) {
            throw new RuntimeException(e);
        }
        for(StatementNode stmt : nodes) {
            if (stmt instanceof CreateTableNode) {
                TableDDL.createTable(ddlFunctions , null, defaultSchema, (CreateTableNode) stmt);
            } else if (stmt instanceof CreateIndexNode) {
                IndexDDL.createIndex(ddlFunctions, null, defaultSchema, (CreateIndexNode) stmt);
            } else if (stmt instanceof CreateViewNode) {
                ViewDDL.createView(ddlFunctions, null, defaultSchema, (CreateViewNode) stmt,
                                   new AISBinderContext(ddlFunctions.getAIS(null), defaultSchema));
            } else {
                throw new IllegalStateException("Unsupported StatementNode type: " + stmt);
            }
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

        public CreateOnlyDDLMock(AkibanInformationSchema ais) {
            this.ais = ais;
        }

        @Override
        public void createTable(Session session, UserTable newTable) {
            AISMerge merge = new AISMerge (ais, newTable);
            merge.merge();
            ais = merge.getAIS();
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
        public void createView(Session session, View view) {
            ais = AISMerge.mergeView(ais, view);
        }

        @Override
        public void dropView(Session session, TableName viewName) {
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

        @Override
        public IndexCheckSummary checkAndFixIndexes(Session session, String schemaRegex, String tableRegex) {
            throw new UnsupportedOperationException();
        }
    }
}
