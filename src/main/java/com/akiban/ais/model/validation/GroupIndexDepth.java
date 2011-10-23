/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

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
