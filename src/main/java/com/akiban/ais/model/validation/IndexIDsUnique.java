
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;

import com.akiban.server.error.DuplicateIndexIdException;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

class IndexIDsUnique implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<Group, Map<Integer, Index>> byGroup = new HashMap<>();
        for (UserTable table : ais.getUserTables().values()) {
            for (Index index : table.getIndexesIncludingInternal()) {
                checkIndexID(byGroup, table.getGroup(), index, output);
            }
        }
        for (Group group : ais.getGroups().values()) {
            for (Index index : group.getIndexes()) {
                checkIndexID(byGroup, group, index, output);
            }
        }
    }
    
    private static void checkIndexID(Map<Group, Map<Integer, Index>> byGroup, Group group, Index index, AISValidationOutput failures) {
        Map<Integer, Index> forGroup = byGroup.get(group);
        if (forGroup == null) {
            forGroup = new TreeMap<>();
            byGroup.put(group, forGroup);
        }
        Index other = forGroup.put(index.getIndexId(), index);
        if (other != null) {
            failures.reportFailure(new AISValidationFailure(new DuplicateIndexIdException(other.getIndexName(), index.getIndexName())));
        }
    }
}
