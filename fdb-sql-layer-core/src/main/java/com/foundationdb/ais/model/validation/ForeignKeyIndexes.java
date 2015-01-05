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
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.ForeignKeyIndexRequiredException;

/**
 * Validates indexes supporting foreign key where found.
 *
 */
class ForeignKeyIndexes implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Table table : ais.getTables().values()) {
            for (ForeignKey foreignKey : table.getForeignKeys()) {
                if (foreignKey.getReferencingTable() == table) { // Only check once
                    if (foreignKey.getReferencingIndex() == null) {
                        output.reportFailure(new AISValidationFailure(new ForeignKeyIndexRequiredException(foreignKey.getConstraintName().getTableName(), false, foreignKey.getReferencingTable().getName(), foreignKey.getReferencingColumns().toString())));
                    }
                    if (foreignKey.getReferencedIndex() == null) {
                        output.reportFailure(new AISValidationFailure(new ForeignKeyIndexRequiredException(foreignKey.getConstraintName().getTableName(), true, foreignKey.getReferencedTable().getName(), foreignKey.getReferencedColumns().toString())));
                    }
                }
            }
        }
    }
}
