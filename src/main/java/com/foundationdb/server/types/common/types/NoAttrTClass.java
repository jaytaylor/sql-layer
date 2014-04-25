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

package com.foundationdb.server.types.common.types;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.sql.types.TypeId;

public class NoAttrTClass extends SimpleDtdTClass {

    @Override
    public TInstance instance(boolean nullable) {
        TInstance result;
        // These two blocks are obviously racy. However, the race will not create incorrectness, it'll just
        // allow there to be multiple copies of the TInstance floating around, each of which is correct, immutable
        // and equivalent.
        if (nullable) {
            result = nullableTInstance;
            if (result == null) {
                result = createInstanceNoArgs(true);
                nullableTInstance = result;
            }
        }
        else {
            result = notNullableTInstance;
            if (result == null) {
                result = createInstanceNoArgs(false);
                notNullableTInstance = result;
            }
        }
        return result;
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        return right; // doesn't matter which!
    }

    @Override
    protected void validate(TInstance type) {
    }

    public TClass widestComparable()
    {
        return this;
    }
    
    public NoAttrTClass(TBundleID bundle, String name, Enum<?> category, TClassFormatter formatter, int internalRepVersion,
                           int serializationVersion, int serializationSize, UnderlyingType underlyingType, TParser parser,
                           int defaultVarcharLen, TypeId typeId) {
        super(bundle, name, category, formatter, Attribute.NONE.class, internalRepVersion, serializationVersion, serializationSize,
                underlyingType, parser, defaultVarcharLen, typeId);
    }

    private volatile TInstance nullableTInstance;
    private volatile TInstance notNullableTInstance;
}
