
package com.akiban.server.encoding;

import com.akiban.ais.model.Column;
import com.akiban.server.rowdata.FieldDef;
import com.persistit.Key;

public interface Encoding {

    /**
     * Size in bytes required to store this value into a RowData. For
     * fixed-length fields this is the field width. For variable-length fields,
     * this is the number of bytes used to store the item, including the number
     * of prefix bytes used to encode its length. For example, a VARCHAR(300)
     * field containing the ASCII string "abc" requires 5 bytes: 3 for the ASCII
     * characters plus two to encode the length value (3).
     *
     * @param fieldDef
     *            description of the field
     * @param value
     *            the value
     * @return size in bytes
     */
    int widthFromObject(final FieldDef fieldDef, final Object value);

    /**
     * Calculate the maximum storage size a given column using this encoding
     * will take when stored in a {@link Key}.
     * @param column column instance
     * @return The maximum storage size.
     */
    long getMaxKeyStorageSize(final Column column);
}
