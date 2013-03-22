/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.collation.InvalidCollationException;
import com.akiban.server.error.UnsupportedCollationException;

/**
 * Verify the table default collation set for each table and column are valid
 * and supported.
 * 
 * @author peter
 * 
 */
class CollationSupported implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            for (Column column : table.getColumnsIncludingInternal()) {
                validateCollationByName(column.getCharsetAndCollation().collation(), table.getName().getSchemaName(),
                        table.getName().getTableName(), column.getName(), output);
            }
        }
    }

    private void validateCollationByName(final String collation, final String schemaName, final String tableName,
            final String columnName, AISValidationOutput output) {
        try {
            AkCollatorFactory.getAkCollator(collation);
        } catch (InvalidCollationException e) {
            output.reportFailure(new AISValidationFailure(new UnsupportedCollationException(schemaName, tableName,
                    columnName, collation)));
        }
    }
}
