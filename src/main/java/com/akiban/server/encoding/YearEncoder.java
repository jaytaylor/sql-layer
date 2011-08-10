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

/**
 * Encoder for working with years when stored as a 1 byte int in the
 * range of 0, 1901-2155.  This is how MySQL stores the SQL YEAR type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/year.html
 */
public final class YearEncoder extends LongEncoderBase {
    YearEncoder() {
    }
}
