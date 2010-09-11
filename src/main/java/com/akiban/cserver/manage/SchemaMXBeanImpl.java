package com.akiban.cserver.manage;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.staticgrouping.Grouping;
import com.akiban.ais.model.staticgrouping.GroupingVisitor;
import com.akiban.ais.model.staticgrouping.GroupingVisitorStub;
import com.akiban.ais.model.staticgrouping.GroupsBuilder;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.CServer;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.RowDef;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SchemaMXBeanImpl implements SchemaManager
{
    private final CServer cserver;

    public SchemaMXBeanImpl(CServer cserver)
    {
        this.cserver = cserver;
    }

    @Override
    public boolean isExperimentalModeActivated()
    {
        return cserver.getStore().isExperimentalSchema();
    }
    
    @Override
    public Map<TableName,String> getUserTables() throws Exception {
        return getTables(true);
    }

    @Override
    public boolean isProtectedTable(int rowDefId) {
        return (rowDefId >= 100000 && rowDefId < 100100 && isExperimentalModeActivated());
    }

    @Override
    public boolean isProtectedTable(String schema, String table) {
        return schema.equals("akiba_information_schema") || schema.equals("akiba_objects");
    }

    /**
     * Gets the DDLs for tables. Eventually we may want to distinguish between user and group tables.
     * @param userTables whether to include user tables (as opposed to group tables)
     * @return a table -> ddl mapping
     * @throws Exception if there's any problem
     */
    private Map<TableName,String> getTables(boolean userTables) throws Exception {
        Map<TableName,String> ret = new TreeMap<TableName, String>();

        StringBuilder builder = new StringBuilder();
        
        for (CServer.CreateTableStruct table : cserver.getStore().getSchema())
        {
            if (table.getSchemaName().equals("akiba_information_schema")) {
                continue;
            }
            if (table.getSchemaName().equals("akiba_objects") == userTables) {
                continue;
            }

            TableName tableName = new TableName(table.getSchemaName(), table.getTableName());
            String createDdl = builder.append("CREATE TABLE ").append(table.getDdl()).toString();
            ret.put(tableName, createDdl);
            builder.setLength(0);
        }

        return ret;
    }

    @Override
    public int getSchemaGenerationID() throws Exception
    {
        return cserver.getStore().getPropertiesManager().getSchemaId().getGeneration();
    }

    @Override
    public void forceSchemaGenerationUpdate() throws Exception {
        cserver.getStore().getPropertiesManager().incrementSchemaId();
    }

    @Override
    public AkibaInformationSchema getAisCopy() {
        return cserver.getAisCopy();
    }

    @Override
    public int createTable(String schemaName, String DDL) throws Exception
    {
        if (schemaName.equals("akiba_information_schema") || schemaName.equals("akiba_objects")) {
            return 0;
        }
        int ret = cserver.getStore().createTable(schemaName, DDL);
        cserver.getStore().getPropertiesManager().incrementSchemaId();
        cserver.acquireAIS();
        return ret;
    }

    @Override
    public int dropTable(String schema, String tableName) throws Exception
    {
        return dropGroups(Arrays.asList(TableName.create(schema, tableName)));
    }

    /**
     * Drops all tables.
     * If this succeeds, the schema generation will be incremented.
     * @return the drop result
     * @throws Exception
     */
    @Override
    public int dropAllTables() throws Exception {
        try {
            final Collection<Integer> dropTables = getTablesToRefIds().values();
            final int result = cserver.getStore().dropTables(dropTables);
            if (result == CServerConstants.OK) {
                cserver.getStore().getPropertiesManager().incrementSchemaId();
            }
            return result;
        }
        finally {
            cserver.acquireAIS();
        }
    }

    private int dropGroups(Collection<TableName> containingTables) throws Exception {
        for (TableName containingTable : containingTables) {
            if (containingTable.getSchemaName().equals("akiba_objects") || containingTable.getSchemaName().equals("akiba_information_schema")) {
                throw new Exception("cannot drop tables in schema " + containingTable.getSchemaName());
            }
        }

        Grouping grouping = GroupsBuilder.fromAis(getAisCopy(), null);
        final Set<String> groupsToDrop = new HashSet<String>();
        final Map<TableName,Integer> tablesToRefIds = getTablesToRefIds();
        for (TableName containingTable : containingTables) {
            if (grouping.containsTable(containingTable)) {
                groupsToDrop.add( grouping.getGroupFor(containingTable).getGroupName() );
            }
        }
        if (groupsToDrop.isEmpty()) {
            return CServerConstants.OK;
        }

        GroupingVisitor<List<Integer>> visitor = new GroupingVisitorStub<List<Integer>>(){
            private final List<Integer> dropTables = new ArrayList<Integer>();
            private boolean shouldDrop;

            @Override
            public void visitGroup(String groupName, TableName rootTable) {
                shouldDrop = groupsToDrop.contains(groupName);
                if (shouldDrop) {
                    dropTable(rootTable);
                }
            }

            @Override
            public boolean startVisitingChildren() {
                return shouldDrop;
            }

            @Override
            public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
                assert shouldDrop
                        : String.format("%s (%s) references %s (%s)", childName, childColumns, parentName, parentColumns);
                dropTable(childName);
            }

            private void dropTable(TableName tableName) {
                dropTables.add( tablesToRefIds.get(tableName) );
            }

            @Override
            public List<Integer> end() {
                return dropTables;
            }
        };

        List<Integer> dropTables = grouping.traverse(visitor);
        try {
            int ret = cserver.getStore().dropTables(dropTables);
            if (ret == CServerConstants.OK) {
                cserver.getStore().getPropertiesManager().incrementSchemaId();
            }
            return ret;
        }
        finally {
            cserver.acquireAIS();
        }
    }

    private Map<TableName,Integer> getTablesToRefIds() {
        List<RowDef> rowDefs = cserver.getRowDefCache().getRowDefs();
        Map<TableName,Integer> ret = new HashMap<TableName, Integer>(rowDefs.size());
        for (RowDef rowDef : rowDefs) {
            TableName tableName = TableName.create(rowDef.getSchemaName(), rowDef.getTableName());
            int id = rowDef.getRowDefId();
            Integer oldId = ret.put(tableName, id);
            assert oldId == null : "duplicate " + oldId + " for " + tableName;
        }
        return ret;
    }

    @Override
    public int dropSchema(String schemaName) throws Exception
    {
        return cserver.getStore().dropSchema(schemaName);
    }

    public String getGrouping() throws Exception {
        AkibaInformationSchema ais = getAisCopy();
        String defaultSchema = null;

        for (UserTable userTable : ais.getUserTables().values()) {
            if (!userTable.getName().getSchemaName().equalsIgnoreCase("akiba_information_schema")) {
                defaultSchema = userTable.getName().getSchemaName();
                break;
            }
        }

        Grouping grouping = GroupsBuilder.fromAis(ais, defaultSchema);

        List<String> groupsToDrop = grouping.traverse(new GroupingVisitorStub<List<String>>() {
            private final List<String> groups = new ArrayList<String>();

            @Override
            public void visitGroup(String groupName, TableName rootTable) {
                if (rootTable.getSchemaName().equalsIgnoreCase("akiba_information_schema")) {
                    groups.add(groupName);
                }
            }

            @Override
            public List<String> end() {
                return groups;
            }
        });

        GroupsBuilder builder = new GroupsBuilder(grouping);
        for (String groupToDrop : groupsToDrop) {
            builder.dropGroup(groupToDrop);
        }

        return grouping.toString();
    }

    @Override
    public List<String> getDDLs() throws Exception {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        writeAIS( printWriter );
        printWriter.flush();
        // StringWriter.flush() is a no-op
        stripSemicolons( stringWriter.getBuffer() );
        String asString = stringWriter.toString();

        List<String> ret = new ArrayList<String>( CServer.getAisDdls() );
        ListIterator<String> iter = ret.listIterator();
        while (iter.hasNext()) {
            final String full = iter.next();
            assert full.charAt( full.length() - 1) == ';' : full;
            final String noSemi = full.substring(0, full.length() - 1);
            iter.set( noSemi.replaceAll("\n", "") );
        }
        ret.add(0, "use `akiba_information_schema`");
        ret.add(0, "create database if not exists `akiba_information_schema`");
        ret.add("create schema if not exists `akiba_objects`");
        
        if (asString.length() > 0) {
            ret.addAll( Arrays.asList(stringWriter.toString().split("\n")) );
        }
        return ret;
    }

    protected void writeAIS(PrintWriter writer) throws Exception {
        AkibaInformationSchema ais = getAisCopy();
        addGroupTables(writer, ais);
        addUserTables(writer);
    }

    private void addGroupTables(PrintWriter writer, AkibaInformationSchema ais) {
        ArrayList<String> ddls = new ArrayList<String>(new DDLGenerator().createAllGroupTables(ais));
        Collections.sort(ddls);

        for (String ddl : ddls) {
            writer.println(ddl);
        }
    }

    private void addUserTables(PrintWriter writer) throws Exception {
        Map<TableName,String> tables = getUserTables();

        String used = null;
        Set<String> createdSchemas = new HashSet<String>(tables.size());

        StringBuilder builder = new StringBuilder();

        for (Map.Entry<TableName,String> ddlEntry : tables.entrySet()) {
            String schema = ddlEntry.getKey().getSchemaName();
            String ddl = ddlEntry.getValue().trim();

            if (!schema.equals(used)) {
                if (createdSchemas.add(schema)) {
                    builder.append("create database if not exists ");
                    TableName.escape(schema, builder);
                    builder.append('\n');
                }
                builder.append("use ");
                TableName.escape(schema, builder);
                writer.println(builder.toString());
                builder.setLength(0);
                used = schema;
            }

            if (ddl.charAt(ddl.length()-1) == ';') {
                ddl = ddl.substring(0, ddl.length()-1);
            }

            writer.println(ddl);
        }
    }

    /**
     * Strips out any semicolon that's immediately followed by a \n.
     * @param buffer the io buffer
     */
    private void stripSemicolons(StringBuffer buffer) {
        for (int index = 0, len=buffer.length(); index < len; ++index) {
            if (buffer.charAt(index) == ';' && (index + 1) < len && buffer.charAt(index+1) == '\n') {
                buffer.deleteCharAt(index);
                --len;
                --index; // so that it stays the same after for's increment expression
            }
        }
    }
}
