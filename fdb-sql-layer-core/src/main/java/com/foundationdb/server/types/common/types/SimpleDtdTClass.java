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
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

public abstract class SimpleDtdTClass extends TClassBase {

    protected <A extends Enum<A> & Attribute>SimpleDtdTClass(TBundleID bundle, String name, Enum<?> category, TClassFormatter formatter, Class<A> enumClass, int internalRepVersion,
                              int serializationVersion, int serializationSize, UnderlyingType underlyingType, TParser parser, int defaultVarcharLen, TypeId typeId) {
        super(bundle, name, category, enumClass, formatter, internalRepVersion, serializationVersion, serializationSize, underlyingType, parser, defaultVarcharLen);
        this.typeId = typeId;
    }

    @Override
    public int jdbcType() {
        return typeId.getJDBCTypeId();
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance type) {
        boolean isNullable = type.nullability(); // on separate line to make NPE easier to catch
        return new DataTypeDescriptor(typeId, isNullable);
    }

    private final TypeId typeId;
}
