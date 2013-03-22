
package com.akiban.server.encoding;

import com.akiban.server.rowdata.FieldDef;

/** Single byte encoding. */
public class SBCSEncoder extends VariableWidthEncoding {

    public static final Encoding INSTANCE = new SBCSEncoder();

    private SBCSEncoder() {
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int prefixWidth = fieldDef.getPrefixSize();
        if (value == null)
            return prefixWidth;
        else
            return value.toString().length() + prefixWidth;
    }
}
