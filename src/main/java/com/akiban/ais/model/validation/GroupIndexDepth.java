
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.server.error.GroupIndexDepthException;

final class GroupIndexDepth implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex index : group.getIndexes()) {
                validate(index, output);
            }
        }
    }

    private static void validate(GroupIndex index, AISValidationOutput output) {
        int bitsNeeded = index.leafMostTable().getDepth() - index.rootMostTable().getDepth() + 1;
        assert bitsNeeded > 0 : index;
        if (bitsNeeded > Long.SIZE) {
            output.reportFailure(new AISValidationFailure(new GroupIndexDepthException(index, bitsNeeded)));
        }
    }
}
