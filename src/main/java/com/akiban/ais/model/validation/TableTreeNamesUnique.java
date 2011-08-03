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
import com.akiban.ais.model.GroupTable;
import com.akiban.server.error.DuplicateTableTreeNamesException;

import java.util.HashMap;
import java.util.Map;

/**
 * Check GroupTable's for unique tree names. Only check the GroupTables because
 * all tables in a group must have the same tree name, see {@link TablesInGroupSameTreeName}
 */
class TableTreeNamesUnique implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<String,GroupTable> treeNameMap = new HashMap<String, GroupTable>();

        for(GroupTable table : ais.getGroupTables().values()) {
            String treeName = table.getTreeName();
            GroupTable curTable = treeNameMap.get(treeName);
            if(curTable != null) {
                output.reportFailure(
                    new AISValidationFailure(
                            new DuplicateTableTreeNamesException (table.getName(), curTable.getName(), treeName)));
            }
            else {
                treeNameMap.put(treeName, table);
            }
        }
    }
}
