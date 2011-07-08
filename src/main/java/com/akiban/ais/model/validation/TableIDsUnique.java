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

import java.util.Map;
import java.util.TreeMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;

public class TableIDsUnique implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<Integer, Table> tableIDList = new TreeMap<Integer, Table>();
        
        for (UserTable table : ais.getUserTables().values()) {
            if (tableIDList.containsKey(table.getTableId())) {
                output.reportFailure(new AISValidationFailure (ErrorCode.DUPLICATE_TABLE, 
                        "Table %s has a duplicate tableID to table %s",
                        table.getName().toString(), 
                        tableIDList.get(table.getTableId()).getName().toString()));
            } else {
                tableIDList.put(table.getTableId(), table);
            }
        }
        for (GroupTable table : ais.getGroupTables().values()) {
            if (tableIDList.containsKey(table.getTableId())) {
                output.reportFailure(new AISValidationFailure (ErrorCode.DUPLICATE_TABLE, 
                        "Table %s has a duplicate tableID to table %s",
                        table.getName().toString(), 
                        tableIDList.get(table.getTableId()).getName().toString()));
            } else {
                tableIDList.put(table.getTableId(), table);
            }
        }
    }
}
