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
package com.foundationdb.server.error;

import java.util.Arrays;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;

public class SetWrongTypeColumns extends InvalidOperationException {
    public SetWrongTypeColumns (RowType rt1, RowType rt2) {
        super(ErrorCode.SET_WRONG_TYPE_COLUMNS,    
                tInstanceOf(rt1),
                tInstanceOf(rt2));
    }

    private static String tInstanceOf (RowType rt) {
        TInstance[] result = new TInstance[rt.nFields()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = rt.typeAt(i);
        }
        return Arrays.toString(result);
    }

    
}
