
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.IndexColumnIsPartialException;

/**
 * Partially indexed columns, see {@link IndexColumn#indexedLength},
 * are not currently supported by the server.
 */
public class IndexColumnIsNotPartial implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(UserTable table : ais.getUserTables().values()) {
            for(Index index : table.getIndexesIncludingInternal()) {
                for(IndexColumn indexColumn : index.getKeyColumns()) {
                    if(indexColumn.getIndexedLength() != null) {
                        output.reportFailure(new AISValidationFailure(
                                new IndexColumnIsPartialException(table.getName(), index.getIndexName().getName(), indexColumn.getPosition())
                        ));
                    }
                }
            }
        }
    }
}
