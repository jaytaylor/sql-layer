
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.GroupMixedTableTypes;

/**
 * Validates that groups do not mix memory tables and Storage tables in the same group.
 * @author tjoneslo
 *
 */
public class MemoryTablesNotMixed implements AISValidation {
    
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Group group : ais.getGroups().values()) {
            validateGroup (ais, group, output);
        }
    }

    private static void validateGroup (AkibanInformationSchema ais, Group group, AISValidationOutput output) {
        UserTable rootTable = group.getRoot();
        if(rootTable == null) {
            return; // Caught elsewhere
        }
        boolean rootMemoryTable = rootTable.hasMemoryTableFactory();
        for (UserTable userTable : ais.getUserTables().values()) {
            if (userTable.getGroup() == group &&
                    userTable.hasMemoryTableFactory() != rootMemoryTable) {
                output.reportFailure(new AISValidationFailure (
                        new GroupMixedTableTypes(group.getName(), rootMemoryTable, userTable.getName())));
            }
        }
    }
}