package com.akiban.cserver.manage;

import com.akiban.ais.model.TableName;

import javax.management.openmbean.TabularData;
import java.util.Map;

public interface SchemaMXBean
{
    static final String SCHEMA_BEAN_NAME = "com.akiban:type=Schema";

    void enableExperimentalMode();
    
    void disableExperimentalMode();

    boolean isExperimentalModeActivated();

    Map<TableName,String> getKnownTables() throws Exception;

    public String getGrouping() throws Exception;
}
