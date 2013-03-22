
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ColumnSizeMismatchException;

public class ColumnMaxAndPrefixSizesMatch implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(UserTable table : ais.getUserTables().values()) {
            for(Column column : table.getColumnsIncludingInternal()) {
                Long maxStorage = column.getMaxStorageSize();
                Long computedMaxStorage = column.computeMaxStorageSize();
                Integer prefix = column.getPrefixSize();
                Integer computedPrefix = column.computePrefixSize();
                if((maxStorage != null) && !maxStorage.equals(computedMaxStorage)) {
                    output.reportFailure(new AISValidationFailure(
                            new ColumnSizeMismatchException(table.getName(), column.getName(),
                                                            "maxStorageSize", maxStorage, computedMaxStorage)
                    ));
                }
                if((prefix != null) && !prefix.equals(computedPrefix)) {
                    output.reportFailure(new AISValidationFailure(
                            new ColumnSizeMismatchException(table.getName(), column.getName(),
                                                            "prefixSize", prefix, computedPrefix)
                    ));
                }
            }
        }
    }
}
