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
import com.foundationdb.server.error.DuplicateIndexTreeNamesException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Check all table and group index tree names for uniqueness.
 */
class IndexTreeNamesUnique implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<String,Index> treeNameMap = new HashMap<>();

        for(Table table : ais.getTables().values()) {
            checkIndexes(output, treeNameMap, table.getIndexesIncludingInternal());
        }

        for(Group group : ais.getGroups().values()) {
            checkIndexes(output, treeNameMap, group.getIndexes());
        }
    }

    private static void checkIndexes(AISValidationOutput output, Map<String, Index> treeNameMap,
                                     Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            String treeName = index.getTreeName();
            Index curIndex = treeNameMap.get(treeName);
            if(curIndex != null) {
                output.reportFailure(
                        new AISValidationFailure(
                                new DuplicateIndexTreeNamesException(index.getIndexName(), curIndex.getIndexName(), treeName)));
            } else {
                treeNameMap.put(treeName, index);
            }
        }
    }
}
