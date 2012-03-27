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

package com.akiban.qp.expression;

import com.akiban.server.types.ValueSource;

public interface BoundExpressions {
    ValueSource eval(int index);

    /**
     * Compares two rows and indicates if and where they differ.
     * @param row The row to be compared to this row.
     * @param leftStartIndex First field to compare in this row.
     * @param rightStartIndex First field to compare in the other row.
     * @param fieldCount Number of fields to compare.
     * @return 0 if all fields are equal. A negative value indicates that this row had the first field
     * that was not equal to the corresponding field in the other row. A positive value indicates that the
     * other row had the first field that was not equal to the corresponding field in this row. In both non-zero
     * cases, the absolute value of the return value is the position of the field that differed, starting the numbering
     * at 1. E.g. a return value of -2 means that the first fields of the rows match, and that in the second field,
     * this row had the smaller value.
     */
    int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount);
}
