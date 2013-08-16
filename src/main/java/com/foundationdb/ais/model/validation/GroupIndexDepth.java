/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.ais.model.validation;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.server.error.GroupIndexDepthException;

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
