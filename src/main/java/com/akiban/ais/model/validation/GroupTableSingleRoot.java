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
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;

public class GroupTableSingleRoot implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Group group : ais.getGroups().values()) {
            validateGroup (ais, group, output);
        }
    }
    
    private void validateGroup (AkibanInformationSchema ais, Group group, AISValidationOutput output) {
        UserTable root = null;
        for (UserTable userTable : ais.getUserTables().values()) {
            if (userTable.getGroup() == group) {
                if (userTable.getParentJoin() == null) {
                    if (root == null) {
                        root = userTable;
                    } else {
                        output.reportFailure(new AISValidationFailure (ErrorCode.GROUP_MULTIPLE_ROOTS,
                                "Group %s has mulitple root tables: %s and %s",
                                root.getName().toString(), 
                                userTable.getName().toString()));
                    }
                }
            }
        }
    }
}
