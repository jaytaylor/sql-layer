
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.TableNotInGroupException;

/**
 * Validates that all tables belong to a group, 
 * All user tables should be in a group.
 * @author tjoneslo
 */
class TablesInAGroup implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            if (table.getGroup() == null) {
                output.reportFailure(new AISValidationFailure (
                        new TableNotInGroupException (table.getName())));
            }
        }
    }
}
