
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.IndexLacksColumnsException;

class IndexHasColumns implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            for (TableIndex index : table.getIndexesIncludingInternal()) {
                if (index.getKeyColumns().size() == 0) {
                    output.reportFailure(new AISValidationFailure (
                            new IndexLacksColumnsException(table.getName(), index.getIndexName().getName())));
                }
            }
        }
    }

}
