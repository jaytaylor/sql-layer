
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.server.error.UnsupportedUniqueGroupIndexException;

public class GroupIndexesNotUnique implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Group group : ais.getGroups().values()) {
            for (Index index : group.getIndexes()) {
                if (index.isUnique()) {
                    output.reportFailure(new AISValidationFailure (
                            new UnsupportedUniqueGroupIndexException(index.getIndexName().getName())));
                }
            }
        }
    }
}
