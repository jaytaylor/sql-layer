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
import com.akiban.ais.model.Column;
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;
/**
 * Verifies that all the table columns are also present in the corresponding group table.
 * 
 * @author tjoneslo
 *
 */
class TableColumnsMatchGroupColumns implements AISValidation {

    NameGenerator name = new DefaultNameGenerator();
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            // table doesn't have a group, this is caught by TablesInAGroup
            if (table.getGroup() == null) { continue; }
            GroupTable groupTable = table.getGroup().getGroupTable();
            
            for (Column column : table.getColumnsIncludingInternal()) {
                if (column.getGroupColumn() == null) {
                    output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                            "Table %s column %s is has no link to Group column",
                            table.getName().toString(), column.getName()));
                }
                String groupColumnName = name.generateColumnName(column);
                if (groupTable.getColumn(groupColumnName) == null) {
                    output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE,
                            "Group table %s is missing user table column %s.%s",
                            groupTable.getName().toString(), table.getName().toString(), column.getName()));
                }
            }
        }
    }
}
