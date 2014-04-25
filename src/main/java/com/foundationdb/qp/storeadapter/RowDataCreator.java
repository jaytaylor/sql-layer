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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;

public final class RowDataCreator {
    public void put(ValueSource source, NewRow into, int f) {

        // TODO efficiency warning
        // NewRow and its users are pretty flexible about types, so let's just convert everything to a String.
        // It's not efficient, but it works.

        if (source.isNull()) {
            into.put(f, null);
            return;
        }
        final Object putObj;
        if (source.hasCacheValue()) {
            putObj = source.getType().typeClass().formatCachedForNiceRow(source);
        }
        else {
            switch (TInstance.underlyingType(source.getType())) {
            case BOOL:
                putObj = source.getBoolean();
                break;
            case INT_8:
                putObj = source.getInt8();
                break;
            case INT_16:
                putObj = source.getInt16();
                break;
            case UINT_16:
                putObj = source.getUInt16();
                break;
            case INT_32:
                putObj = source.getInt32();
                break;
            case INT_64:
                putObj = source.getInt64();
                break;
            case FLOAT:
                putObj = source.getFloat();
                break;
            case DOUBLE:
                putObj = source.getDouble();
                break;
            case STRING:
                putObj = source.getString();
                break;
            case BYTES:
                putObj = source.getBytes();
                break;
            default:
                throw new AssertionError(source.getType());
            }
        }
        into.put(f, putObj);
    }
}
