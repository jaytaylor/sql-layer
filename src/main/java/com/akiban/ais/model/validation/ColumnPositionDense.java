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
import com.akiban.ais.model.Table;
import com.akiban.server.error.ColumnPositionNotOrderedException;

class ColumnPositionDense implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Table table : ais.getUserTables().values()) {
            checkTable (table, output);
        }
        
        for (Table table : ais.getGroupTables().values()) {
            checkTable (table, output);
        }
    }

    private void checkTable (Table table, AISValidationOutput output) {
        for (int i = 0; i < table.getColumnsIncludingInternal().size(); i++) {
            if (table.getColumnsIncludingInternal().get(i).getPosition() != i) {
                output.reportFailure(new AISValidationFailure(
                        new ColumnPositionNotOrderedException(table.getName(), 
                                table.getColumn(i).getName(), 
                                table.getColumn(i).getPosition(),
                                i)));
            }
        }
    }
}
