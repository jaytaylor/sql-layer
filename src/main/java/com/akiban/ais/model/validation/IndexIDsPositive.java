
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.InvalidIndexIDException;

import java.util.Collection;

/**
 * <p>The index ID of zero is special for MySQL adapter. It signifies a
 * full group scan ("HKEY" scan) and should not overlap with a real ID.</p>
 *
 * <p>There are also arrays sized to the max index ID so reject negative too.</p>
 */
public class IndexIDsPositive implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(UserTable table : ais.getUserTables().values()) {
            checkIDs(table.getIndexesIncludingInternal(), output);
        }
        for(Group group : ais.getGroups().values()) {
            checkIDs(group.getIndexes(), output);
        }
    }

    private static void checkIDs(Collection<? extends Index> indexes, AISValidationOutput output) {
        for(Index index : indexes) {
            int id = index.getIndexId();
            if(id <= 0) {
                output.reportFailure(new AISValidationFailure(new InvalidIndexIDException(index.getIndexName(), id)));
            }
        }
    }
}
