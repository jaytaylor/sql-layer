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

package com.akiban.server.encoding;

import com.akiban.ais.model.Type;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.rowdata.RowData;
import com.akiban.util.AkibanAppender;

/**
 * Encoder for working with dates when stored as a 3 byte int using
 * the encoding of DD + MM x 32 + YYYY x 512. This is how MySQL stores the
 * SQL DATE type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
 */
public final class DateEncoder extends LongEncoderBase {
    DateEncoder() {
    }
}
