
package com.akiban.server.service.dxl;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;

import java.util.List;

@SuppressWarnings("unused") // jmx
public interface DXLMXBean {
    String getUsingSchema();

    void recreateGroupIndexes();

    void setUsingSchema(String schema);

    void createGroupIndex(String schemaName, String groupName, String indexName, String tableColumnList, Index.JoinType joinType);

    void dropTable(String tableName) ;

    void dropGroupIndex(String schemaName, String groupName, String indexName) ;

    void dropGroup(String schemaName, String groupName) ;
    
    void dropGroupBySchema(String schemaName) ;

    void dropAllGroups() ;

    void writeRow(String table, String fields) ;

    List<String> getGrouping();

    List<String> getGroupIndexDDLs();

    TableName getGroupNameFromTableName(String schema, String table);

    String printAIS();

    public IndexCheckSummary checkAndFix(String schemaRegex, String tableRegex);

    public IndexCheckSummary checkAndFixAll();
}
