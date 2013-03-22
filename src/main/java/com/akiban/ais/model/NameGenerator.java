
package com.akiban.ais.model;

import java.util.List;
import java.util.Set;

public interface NameGenerator
{
    // Generation
    int generateTableID(TableName name);
    int generateIndexID(int rootTableID);
    TableName generateIdentitySequenceName(TableName table);
    String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> joinIndex);
    String generateJoinName(TableName parentTable, TableName childTable, List<String> pkColNames, List<String> fkColNames);
    String generateIndexTreeName(Index index);
    String generateGroupTreeName(String schemaName, String groupName);
    String generateSequenceTreeName(Sequence sequence);

    // Removal
    void removeTableID(int tableID);
    void removeTreeName(String treeName);

    // View only (debug/testing)
    Set<String> getTreeNames();
}
