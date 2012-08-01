/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types3.aksql.aktypes;

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.IllegalNameException;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TClassBase;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TParser;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import java.util.HashMap;
import java.util.Map;

public abstract class IntervalBase extends TClassBase {

    @Override
    public void attributeToString(int attributeIndex, long value, StringBuilder output) {
        if (attributeIndex == formatAttribute.ordinal())
            attributeToString(formatters, value, output);
        else
            super.attributeToString(attributeIndex, value,  output);
    }

    @Override
    protected TInstance doPickInstance(TInstance instance0, TInstance instance1) {
        return instance();
    }

    @Override
    public TInstance instance() {
        return instance(formatAttribute.ordinal());
    }

    @Override
    protected void validate(TInstance instance) {
        int formatId = instance.attribute(formatAttribute);
        if ( (formatId < 0) || (formatId >= formatters.length) )
            throw new IllegalNameException("unrecognized literal format ID: " + formatId);
    }

    @Override
    public void putSafety(TExecutionContext context, TInstance sourceInstance, PValueSource sourceValue,
                          TInstance targetInstance, PValueTarget targetValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        Boolean isNullable = instance.nullability(); // on separate line to make NPE easier to catch
        int literalFormatId = instance.attribute(formatAttribute);
        IntervalFormat format = formatters[literalFormatId];
        TypeId typeId = format.getTypeId();
        return new DataTypeDescriptor(typeId, isNullable);
    }

    @Override
    public TFactory factory() {
        throw new UnsupportedOperationException();
    }

    public TInstance tInstanceFrom(DataTypeDescriptor type) {
        TypeId typeId = type.getTypeId();
        IntervalFormat format = typeIdToFormat.get(typeId);
        if (format == null)
            throw new IllegalArgumentException("couldn't convert " + type + " to " + name());
        TInstance result = instance(format.ordinal());
        result.setNullable(type.isNullable());
        return result;
    }

    <A extends Enum<A> & Attribute> IntervalBase(TBundleID bundle, String name,
                                                           Enum<?> category, Class<A> enumClass,
                                                           TClassFormatter formatter,
                                                           int internalRepVersion, int sVersion, int sSize,
                                                           PUnderlying pUnderlying,
                                                           A formatAttribute,
                                                           IntervalFormat[] formatters)
    {
        super(bundle, name, category, enumClass, formatter, internalRepVersion, sVersion, sSize, pUnderlying,
                createParser(formatAttribute, formatters));
        this.formatters = formatters;
        this.formatAttribute = formatAttribute;
        this.typeIdToFormat = createTypeIdToFormatMap(formatters);

    }

    private final IntervalFormat[] formatters;
    private final Attribute formatAttribute;
    private final Map<TypeId,IntervalFormat> typeIdToFormat;

    private static <F extends IntervalFormat> TParser createParser(final Attribute formatAttribute,
                                                                   final F[] formatters)
    {
        return new TParser() {
            @Override
            public void parse(TExecutionContext context, PValueSource in, PValueTarget out) {
                TInstance instance = context.outputTInstance();
                int literalFormatId = instance.attribute(formatAttribute);
                F format = formatters[literalFormatId];
                String inString = in.getString();
                long months = format.parse(inString);
                out.putInt64(months);
            }
        };
    }

    private static <F extends IntervalFormat> Map<TypeId, F> createTypeIdToFormatMap(F[] values) {
        Map<TypeId, F> map = new HashMap<TypeId, F>(values.length);
        for (F literalFormat : values)
            map.put(literalFormat.getTypeId(), literalFormat);
        return map;
    }
}
