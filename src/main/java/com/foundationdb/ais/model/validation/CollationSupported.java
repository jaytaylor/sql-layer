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
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.UnsupportedCollationException;

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
        for (Table table : ais.getTables().values()) {
            for (Column column : table.getColumnsIncludingInternal()) {
                validateCollationByName(column.getCollationName(), table.getName().getSchemaName(),
                        table.getName().getTableName(), column.getName(), output);
            }
        }
    }

    private void validateCollationByName(final String collation, final String schemaName, final String tableName,
            final String columnName, AISValidationOutput output) {
        try {
            AkCollatorFactory.getAkCollator(collation);
        } catch (UnsupportedCollationException e) {
            output.reportFailure(new AISValidationFailure(new UnsupportedCollationException(collation)));
        }
    }
}
