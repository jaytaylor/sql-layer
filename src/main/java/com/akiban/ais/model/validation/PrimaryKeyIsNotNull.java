
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.PrimaryKeyNullColumnException;

/**
 * Validates that the columns used in the primary key are all not null. 
 * This is a requirement for the Derby (but not enforced except here).
 * MySQL enforces this by silently making the columns not null.   
 * @author tjoneslo
 *
 */
class PrimaryKeyIsNotNull implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            PrimaryKey index = table.getPrimaryKeyIncludingInternal();
            if(index == null) {
                continue; // Checked by TableHasPrimaryKey
            }
            for (Column column : index.getColumns()) {
                if (column.getNullable()) {
                    output.reportFailure(new AISValidationFailure (
                            new PrimaryKeyNullColumnException (table.getName(), column.getName())));
                }
            }
        }
    }
}
