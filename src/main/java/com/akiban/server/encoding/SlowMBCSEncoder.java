
package com.akiban.server.encoding;

import com.akiban.ais.model.TableName;
import com.akiban.server.error.UnsupportedCharsetException;
import com.akiban.server.rowdata.FieldDef;

import java.io.UnsupportedEncodingException;

/** General multi-byte byte encoding. Don't know what it will do, so
 * have to go through the full conversion to bytes only to get the
 * length.
*/
public class SlowMBCSEncoder extends VariableWidthEncoding {
    private final String charset;
    
    public SlowMBCSEncoder(String charset) {
        this.charset = charset;
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int prefixWidth = fieldDef.getPrefixSize();
        if (value == null)
            return prefixWidth;
        else {
            String charsetName = fieldDef.column().getCharsetAndCollation().charset();
            try {
                return value.toString().getBytes(charsetName).length + prefixWidth;
            }
            catch (UnsupportedEncodingException ex) {
                TableName table = fieldDef.column().getTable().getName();
                throw new UnsupportedCharsetException(table.getSchemaName(), table.getTableName(), charsetName);
            }
        }
    }
}
