package com.akiban.server.types3.common.types;

import com.akiban.server.types3.*;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

public abstract class SimpleDtdTClass extends TClassBase {

    protected <A extends Enum<A> & Attribute>SimpleDtdTClass(TBundleID bundle, String name, Enum<?> category, TClassFormatter formatter, Class<A> enumClass, int internalRepVersion,
                              int serializationVersion, int serializationSize, PUnderlying pUnderlying, TParser parser, int defaultVarcharLen, TypeId typeId) {
        super(bundle, name, category, enumClass, formatter, internalRepVersion, serializationVersion, serializationSize, pUnderlying, parser, defaultVarcharLen);
        this.typeId = typeId;
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        boolean isNullable = instance.nullability(); // on separate line to make NPE easier to catch
        return new DataTypeDescriptor(typeId, isNullable);
    }

    private final TypeId typeId;
}
