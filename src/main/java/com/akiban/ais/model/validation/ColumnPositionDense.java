
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Table;
import com.akiban.server.error.ColumnPositionNotOrderedException;

class ColumnPositionDense implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Table table : ais.getUserTables().values()) {
            checkTable (table, output);
        }
    }

    private void checkTable (Table table, AISValidationOutput output) {
        for (int i = 0; i < table.getColumnsIncludingInternal().size(); i++) {
            if (table.getColumnsIncludingInternal().get(i).getPosition() != i) {
                output.reportFailure(new AISValidationFailure(
                        new ColumnPositionNotOrderedException(table.getName(), 
                                table.getColumn(i).getName(), 
                                table.getColumn(i).getPosition(),
                                i)));
            }
        }
    }
}
