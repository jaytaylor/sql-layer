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
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.server.error.InvalidIndexIDException;

import java.util.Collection;

/**
 * <p>The index ID of zero is special for MySQL adapter. It signifies a
 * full group scan ("HKEY" scan) and should not overlap with a real ID.</p>
 *
 * <p>There are also arrays sized to the max index ID so reject negative too.</p>
 */
public class IndexIDsPositive implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(UserTable table : ais.getUserTables().values()) {
            checkIDs(table.getIndexesIncludingInternal(), output);
        }
        for(Group group : ais.getGroups().values()) {
            checkIDs(group.getIndexes(), output);
        }
    }

    private static void checkIDs(Collection<? extends Index> indexes, AISValidationOutput output) {
        for(Index index : indexes) {
            int id = index.getIndexId();
            if(id <= 0) {
                output.reportFailure(new AISValidationFailure(new InvalidIndexIDException(index.getIndexName(), id)));
            }
        }
    }
}
