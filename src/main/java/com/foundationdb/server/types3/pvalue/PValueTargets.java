/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.types3.pvalue;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.DeepCopiable;
import com.akiban.server.types3.TInstance;

public final class PValueTargets {
    private PValueTargets() {}

    public static void putLong(PValueTarget target, long val)
    {
        switch (pUnderlying(target))
        {
            case INT_8:
                target.putInt8((byte)val);
                break;
            case INT_16:
                target.putInt16((short)val);
                break;
            case INT_32:
                target.putInt32((int)val);
                break;
            case INT_64:
                target.putInt64(val);
                break;
            default:
                throw new AkibanInternalException("Cannot put LONG into " + target.tInstance());
        }
    }
    
    public static PUnderlying pUnderlying(PValueTarget valueTarget){
        return TInstance.pUnderlying(valueTarget.tInstance());
    }

    public static void copyFrom(PValueSource source, PValueTarget target) {
        if (source.isNull()) {
            target.putNull();
            return;
        }
        else if (source.hasCacheValue()) {
            if (target.supportsCachedObjects()) {
                // The BigDecimalWrapper is mutable
                // a shalloow copy won't work.
                Object obj = source.getObject();
                if (obj instanceof DeepCopiable)
                    target.putObject(((DeepCopiable)obj).deepCopy());
                else
                    target.putObject(source.getObject());
                return;
            }
            else if (!source.canGetRawValue()) {
                throw new IllegalStateException("source has only cached object, but no cacher provided: " + source);
            }
        }
        else if (!source.canGetRawValue()) {
            throw new IllegalStateException("source has no value: " + source);
        }
        switch (TInstance.pUnderlying(source.tInstance())) {
        case BOOL:
            target.putBool(source.getBoolean());
            break;
        case INT_8:
            target.putInt8(source.getInt8());
            break;
        case INT_16:
            target.putInt16(source.getInt16());
            break;
        case UINT_16:
            target.putUInt16(source.getUInt16());
            break;
        case INT_32:
            target.putInt32(source.getInt32());
            break;
        case INT_64:
            target.putInt64(source.getInt64());
            break;
        case FLOAT:
            target.putFloat(source.getFloat());
            break;
        case DOUBLE:
            target.putDouble(source.getDouble());
            break;
        case BYTES:
            target.putBytes(source.getBytes());
            break;
        case STRING:
            target.putString(source.getString(), null);
            break;
        default:
            throw new AssertionError(source.tInstance());
        }
    }


}
