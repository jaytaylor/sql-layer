
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.DuplicateIndexTreeNamesException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Check all table and group index tree names for uniqueness.
 */
class IndexTreeNamesUnique implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<String,Index> treeNameMap = new HashMap<>();

        for(UserTable table : ais.getUserTables().values()) {
            checkIndexes(output, treeNameMap, table.getIndexesIncludingInternal());
        }

        for(Group group : ais.getGroups().values()) {
            checkIndexes(output, treeNameMap, group.getIndexes());
        }
    }

    private static void checkIndexes(AISValidationOutput output, Map<String, Index> treeNameMap,
                                     Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            String treeName = index.getTreeName();
            Index curIndex = treeNameMap.get(treeName);
            if(curIndex != null) {
                output.reportFailure(
                        new AISValidationFailure(
                                new DuplicateIndexTreeNamesException(index.getIndexName(), curIndex.getIndexName(), treeName)));
            } else {
                treeNameMap.put(treeName, index);
            }
        }
    }
}
