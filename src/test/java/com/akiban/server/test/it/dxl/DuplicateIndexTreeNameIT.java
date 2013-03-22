
package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.*;
import com.akiban.ais.model.validation.AISValidation;
import com.akiban.ais.model.validation.AISValidationFailure;
import com.akiban.ais.model.validation.AISValidationOutput;
import com.akiban.ais.model.validation.AISValidationResults;
import com.akiban.server.error.DuplicateIndexTreeNamesException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

// Inspired by bug 873070

public class DuplicateIndexTreeNameIT extends ITBase
{
    @Test
    public void createRenameCreate()
    {
        createTable("schema", "root", "id int not null, primary key(id)");
        createTable("schema", "child", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        ddl().renameTable(session(), tableName("schema", "child"), tableName("schema", "renamed_child"));
        createTable("schema", "child", "id int not null, rid int, primary key(id)", akibanFK("rid", "root", "id"));
        AkibanInformationSchema ais = ddl().getAIS(session());
        AISValidationResults results = ais.validate(Collections.singleton((AISValidation)new  IndexTreeNamesUnique()));
        assertEquals(0, results.failures().size());
    }

    // Copy of class in com.akiban.ais.model.validation, but it isn't public.
    static class IndexTreeNamesUnique implements AISValidation
    {

        @Override
        public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
            Map<String,Index> treeNameMap = new HashMap<>();

            for(UserTable table : ais.getUserTables().values()) {
                checkIndexes(output, treeNameMap, table.getIndexes());
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
}
