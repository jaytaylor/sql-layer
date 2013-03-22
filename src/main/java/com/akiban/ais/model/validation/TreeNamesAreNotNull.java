
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.GroupTreeNameIsNullException;
import com.akiban.server.error.IndexTreeNameIsNullException;
import com.akiban.server.error.SequenceTreeNameIsNullException;

import java.util.Collection;

/**
 * Check all group and index tree names are not null.
 */
public class TreeNamesAreNotNull implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(UserTable table : ais.getUserTables().values()) {
            checkTable(table);
        }
        for(Group group : ais.getGroups().values()) {
            checkGroup(group);
            for(Index index : group.getIndexes()) {
                checkIndex(index);
            }
        }
        for (Sequence sequence: ais.getSequences().values()) {
            checkSequence(sequence);
        }
    }

    private static void checkGroup(Group group) {
        if(group.getTreeName() == null) {
            throw new GroupTreeNameIsNullException(group);
        }
    }

    private static void checkTable(Table table) {
        final Collection<TableIndex> indexes;
        if(table.isUserTable()) {
            indexes = ((UserTable)table).getIndexesIncludingInternal();
        } else {
            indexes = table.getIndexes();
        }
        for(Index index : indexes) {
            checkIndex(index);
        }
    }

    private static void checkIndex(Index index) {
        if(index.getTreeName() == null) {
            throw new IndexTreeNameIsNullException(index);
        }
    }
    
    private static void checkSequence(Sequence sequence) {
        if (sequence.getTreeName() == null) {
            throw new SequenceTreeNameIsNullException(sequence);
        }
    }
}
