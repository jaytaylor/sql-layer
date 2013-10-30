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
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.AISValidationException;

/** Every table and every column has a UUID. */
public class UUIDPresent implements AISValidation
{
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(Table t : ais.getTables().values()) {
            if(t.getUuid() == null) {
                output.reportFailure(new AISValidationFailure(missingTable(t)));
            }
            for(Column c: t.getColumnsIncludingInternal()) {
                if(c.getUuid() == null) {
                    output.reportFailure(new AISValidationFailure(missingColumn(t, c)));
                }
            }
        }
    }

    private AISValidationException missingTable(Table t) {
        return new AISValidationException(String.format("Table %s missing UUID", t.getName()));
    }

    private AISValidationException missingColumn(Table t, Column c) {
        return new AISValidationException(String.format("Column %s.%s missing UUID", t.getName(), c.getName()));
    }
}
