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

import java.nio.charset.Charset;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.UnsupportedCharsetException;

/**
 * Verify the table default character set and define character sets for each column
 * are valid and supported. 
 * @author tjoneslo
 *
 */
class CharacterSetSupported implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            final String tableCharset = table.getCharsetAndCollation().charset(); 
            if (tableCharset != null && !Charset.isSupported(tableCharset)) {
                output.reportFailure(new AISValidationFailure (
                        new UnsupportedCharsetException (table.getName().getSchemaName(),
                                table.getName().getTableName(), tableCharset)));
            }
            
            for (Column column : table.getColumnsIncludingInternal()) {
                final String columnCharset = column.getCharsetAndCollation().charset();
                if (columnCharset != null && !Charset.isSupported(columnCharset)) {
                    output.reportFailure(new AISValidationFailure (
                            new UnsupportedCharsetException (table.getName().getSchemaName(),
                                    table.getName().getTableName(), columnCharset)));
                }
            }
        }
    }
}
