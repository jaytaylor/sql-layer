
package com.akiban.ais.model.validation;

import java.nio.charset.Charset;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
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
