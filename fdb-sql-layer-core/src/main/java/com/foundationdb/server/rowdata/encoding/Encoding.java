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

package com.foundationdb.server.rowdata.encoding;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.rowdata.FieldDef;
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
