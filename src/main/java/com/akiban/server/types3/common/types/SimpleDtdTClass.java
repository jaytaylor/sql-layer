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

package com.akiban.server.types3.common.types;

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TName;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

public abstract class SimpleDtdTClass extends TClass {
    protected <A extends Enum<A> & Attribute> SimpleDtdTClass(TName name, Class<A> enumClass, int internalRepVersion, int serializationVersion,
                              int serializationSize, PUnderlying pUnderlying, TypeId typeId) {
        super(name, enumClass, internalRepVersion, serializationVersion, serializationSize, pUnderlying);
        this.typeId = typeId;
    }

    protected <A extends Enum<A> & Attribute>SimpleDtdTClass(TBundleID bundle, String name, Class<A> enumClass, int internalRepVersion,
                              int serializationVersion, int serializationSize, PUnderlying pUnderlying, TypeId typeId) {
        super(bundle, name, enumClass, internalRepVersion, serializationVersion, serializationSize, pUnderlying);
        this.typeId = typeId;
    }

    @Override
    public DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        boolean isNullable = instance.nullability(); // on separate line to make NPE easier to catch
        return new DataTypeDescriptor(typeId, isNullable);
    }

    private final TypeId typeId;
}
