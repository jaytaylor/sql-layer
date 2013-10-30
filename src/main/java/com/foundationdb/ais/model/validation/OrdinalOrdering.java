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
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.AISValidationException;

/** Every table has an ordinal. If a table is a child, it's ordinal is greater than the parent's. */
public class OrdinalOrdering implements AISValidation
{
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(Table t : ais.getTables().values()) {
            if(t.getOrdinal() == null) {
                output.reportFailure(new AISValidationFailure(noOrdinal(t)));
            } else {
                Table p = t.getParentTable();
                if((p != null) && (p.getOrdinal() != null) && (t.getOrdinal() < p.getOrdinal())) {
                    output.reportFailure(new AISValidationFailure(lowerOrdinal(p, t)));
                }
            }
        }
    }

    private static AISValidationException noOrdinal(Table t) {
        return new AISValidationException(
            String.format("Table %s has no ordinal", t.getName())
        );
    }

    private static AISValidationException lowerOrdinal(Table parent, Table child) {
        return new AISValidationException(
            String.format("Table %s has ordinal %d lower than parent %s ordinal %d",
                          child.getName(), child.getOrdinal(), parent.getName(), parent.getOrdinal())
        );
    }
}
