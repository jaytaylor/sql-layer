
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.NoPrimaryKeyException;

class TableHasPrimaryKey implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            if (table.getPrimaryKeyIncludingInternal() == null) {
                output.reportFailure(new AISValidationFailure (
                        new NoPrimaryKeyException(table.getName())));
            }
        }
    }
}
