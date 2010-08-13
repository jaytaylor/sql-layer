package com.akiban.cserver.manage;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.staticgrouping.GroupsBuilder;
import com.akiban.cserver.CServer;

import java.util.HashMap;
import java.util.Map;

public final class SchemaMXBeanImpl implements SchemaMXBean
{
    private final CServer cserver;

    public SchemaMXBeanImpl(CServer cserver)
    {
        this.cserver = cserver;
    }

    @Override
    public void disableExperimentalMode()
    {
        cserver.getStore().setExperimental("");
    }

    @Override
    public void enableExperimentalMode()
    {
        cserver.getStore().setExperimental("schema");
    }
    
    @Override
    public boolean isExperimentalModeActivated()
    {
        return cserver.getStore().isExperimentalSchema();
    }
    
    @Override
    public Map<TableName,String> getKnownTables() throws Exception
    {
        Map<TableName,String> ret = new HashMap<TableName, String>();

        StringBuilder builder = new StringBuilder();
        cserver.acquireAIS();
        for (CServer.CreateTableStruct table : cserver.getStore().getSchema())
        {
            TableName tableName = new TableName(table.getSchemaName(), table.getTableName());
            ret.put(tableName, builder.append("CREATE TABLE ").append(table.getDdl()).toString());
            builder.setLength(0);
        }

        return ret;
    }

    public String getGrouping() throws Exception {
        cserver.acquireAIS();
        AkibaInformationSchema ais = cserver.getAisCopy();
        String defaultSchema = null;
        if (!ais.getUserTables().isEmpty()) {
            defaultSchema = ais.getUserTables().values().iterator().next().getName().getSchemaName();
        }
        return GroupsBuilder.fromAis(ais, defaultSchema).toString();
    }
}
