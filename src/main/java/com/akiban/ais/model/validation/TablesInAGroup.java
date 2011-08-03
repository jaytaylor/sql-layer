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
import com.akiban.server.error.ErrorCode;
/**
 * Validates that all tables belong to a group, 
 * All user tables should be in a group.
 * All group tables should belong to a group. 
 * @author tjoneslo
 *
 */
class TablesInAGroup implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            if (table.getGroup() == null) {
                output.reportFailure(new AISValidationFailure (ErrorCode.INTERNAL_REFERENCES_BROKEN,
                        "Table %s does not connect to a group", table.getName().toString()));
            }
        }
        for (GroupTable table : ais.getGroupTables().values()) {
            if (table.getGroup() == null) {
                output.reportFailure(new AISValidationFailure (ErrorCode.INTERNAL_REFERENCES_BROKEN,
                        "Table %s does not connect to a group", table.getName().toString()));
            }
        }
    }
}
