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
import com.foundationdb.ais.model.Table;

import com.foundationdb.server.error.DuplicateIndexIdException;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

class IndexIDsUnique implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<Group, Map<Integer, Index>> byGroup = new HashMap<>();
        for (Table table : ais.getTables().values()) {
            for (Index index : table.getIndexesIncludingInternal()) {
                checkIndexID(byGroup, table.getGroup(), index, output);
            }
        }
        for (Group group : ais.getGroups().values()) {
            for (Index index : group.getIndexes()) {
                checkIndexID(byGroup, group, index, output);
            }
        }
    }
    
    private static void checkIndexID(Map<Group, Map<Integer, Index>> byGroup, Group group, Index index, AISValidationOutput failures) {
        Map<Integer, Index> forGroup = byGroup.get(group);
        if (forGroup == null) {
            forGroup = new TreeMap<>();
            byGroup.put(group, forGroup);
        }
        Index other = forGroup.put(index.getIndexId(), index);
        if (other != null) {
            failures.reportFailure(new AISValidationFailure(new DuplicateIndexIdException(other.getIndexName(), index.getIndexName())));
        }
    }
}
