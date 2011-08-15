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
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.TreeNameMismatchException;

/**
 * All UserTable's in a given Group must have the same tree name as that groups GroupTable.
 */
class TablesInGroupSameTreeName implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(UserTable userTable : ais.getUserTables().values()) {
            GroupTable groupTable = userTable.getGroup() != null ? userTable.getGroup().getGroupTable() : null;
            if(groupTable != null) {
                String userTableTreeName = userTable.getTreeName();
                String groupTableTreeName = groupTable.getTreeName();
                if(!userTableTreeName.equals(groupTableTreeName)) {
                    output.reportFailure(
                        new AISValidationFailure(
                                new TreeNameMismatchException(userTable.getName(), userTableTreeName, groupTableTreeName)));
                }
            }
            // else: handled elsewhere, TablesInAGroup
        }
    }
}
