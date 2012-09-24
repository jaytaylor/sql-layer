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

package com.akiban.server.types3;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueCacher;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ArgumentValidation;
import com.google.common.base.Objects;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public abstract class TClass {

    public abstract TFactory factory();

    protected abstract DataTypeDescriptor dataTypeDescriptor(TInstance instance);

    public abstract void fromObject (TExecutionContext contextForErrors, PValueSource in, PValueTarget out);

    public abstract TCast castToVarchar();
    public abstract TCast castFromVarchar();

    public void selfCast(TExecutionContext context,
                         TInstance sourceInstance, PValueSource source, TInstance targetInstance, PValueTarget target) {
        PValueTargets.copyFrom(source, target);
    }

    public boolean normalizeInstancesBeforeComparison() {
        return false;
    }

    public static boolean comparisonNeedsCasting(TInstance left, TInstance right) {
        if (left == null || right == null)
            return true;
        TClass leftClass = left.typeClass();
        TClass rightClass = right.typeClass();
        return  (leftClass != rightClass)
                || (leftClass.normalizeInstancesBeforeComparison() && (!left.equalsExcludingNullable(right)));
    }

    public static int compare(TInstance instanceA, PValueSource sourceA, TInstance instanceB, PValueSource sourceB) {
        if (comparisonNeedsCasting(instanceA, instanceB))
            throw new IllegalArgumentException("can't compare " + instanceA + " and " + instanceB);

        if (sourceA.isNull())
            return sourceB.isNull() ? 0 : -1;
        if (sourceB.isNull())
            return 1;
        return instanceA.typeClass().doCompare(instanceA, sourceA, instanceB, sourceB);
    }

    /**
     * Compares two values, assuming neither is null. The call site (<tt>TClass.compare</tt>) will handle the case
     * that either or both sources is null.
     * @param instanceA the first operand's instance
     * @param sourceA the first operand's value, which will not represent a null PValueSource
     * @param instanceB the second operand's instance
     * @param sourceB the second operand's value, which will not represent a null PValueSource
     * @return -1 if sourceA is less than sourceB; 0 if they're equal; 1 if sourceA is greater than sourceB
     * @see TClass#compare(TInstance, PValueSource, TInstance, PValueSource)
     */
    protected int doCompare(TInstance instanceA, PValueSource sourceA, TInstance instanceB, PValueSource sourceB) {
        if (sourceA.hasCacheValue() && sourceB.hasCacheValue()) {
            Object objectA = sourceA.getObject();
            if (objectA instanceof Comparable<?>) {
                // assume objectA and objectB are of the same class. If it's comparable, use that
                Comparable comparableA = (Comparable<?>) objectA;
                return comparableA.compareTo(sourceB.getObject());
            }
        }
        ensureRawValue(sourceA, instanceA);
        ensureRawValue(sourceB, instanceB);
        switch (sourceA.getUnderlyingType()) {
        case BOOL:
            return Booleans.compare(sourceA.getBoolean(), sourceB.getBoolean());
        case INT_8:
            return sourceA.getInt8() - sourceB.getInt8();
        case INT_16:
            return sourceA.getInt16() - sourceB.getInt16();
        case UINT_16:
            return sourceA.getUInt16() - sourceB.getUInt16();
        case INT_32:
            return sourceA.getInt32() - sourceB.getInt32();
        case INT_64:
            return Longs.compare(sourceA.getInt64(), sourceB.getInt64());
        case FLOAT:
            return Floats.compare(sourceA.getFloat(), sourceB.getFloat());
        case DOUBLE:
            return Doubles.compare(sourceA.getDouble(), sourceB.getDouble());
        case BYTES:
            ByteBuffer bbA = ByteBuffer.wrap(sourceA.getBytes());
            ByteBuffer bbB = ByteBuffer.wrap(sourceB.getBytes());
            return bbA.compareTo(bbB);
        case STRING:
            return sourceA.getString().compareTo(sourceB.getString());
        default:
            throw new AssertionError(sourceA.getUnderlyingType());
        }
    }

    protected void writeCanonical(PValueSource in, TInstance typeInstance, PValueTarget out) {
        PValueTargets.copyFrom(in, out);
    }

    public void attributeToString(int attributeIndex, long value, StringBuilder output) {
        output.append(value);
    }

    protected static void attributeToString(Object[] array, long arrayIndex, StringBuilder output) {
        if ( (array == null) || (arrayIndex < 0) || arrayIndex >= array.length)
            output.append(arrayIndex);
        else
            output.append(array[(int)arrayIndex]);
    }

    public abstract void putSafety(TExecutionContext context,
                        TInstance sourceInstance,
                        PValueSource sourceValue,
                        TInstance targetInstance,
                        PValueTarget targetValue);

    public abstract TInstance instance();

    public TInstance instance(int arg0)
    {
        return createInstance(1, arg0, EMPTY, EMPTY, EMPTY);
    }

    public TInstance instance(int arg0, int arg1)
    {
        return createInstance(2, arg0, arg1, EMPTY, EMPTY);
    }

    public TInstance instance(int arg0, int arg1, int arg2)
    {
        return createInstance(3, arg0, arg1, arg2, EMPTY);
    }

    public TInstance instance(int arg0, int arg1, int arg2, int arg3)
    {
        return createInstance(4, arg0, arg1, arg2, arg3);
    }

    protected void writeCollating(PValueSource inValue, TInstance inInstance, PValueTarget out) {
        writeCanonical(inValue, inInstance, out);
    }

    protected void readCanonical(PValueSource inValue, TInstance typeInstance, PValueTarget out) {
        writeCanonical(inValue, typeInstance, out);
    }

    public TInstance pickInstance(TInstance instance0, TInstance instance1) {
        if (instance0.typeClass() != this || instance1.typeClass() != this)
            throw new IllegalArgumentException("can't combine " + instance0 + " and " + instance1 + " using " + this);

        TInstance result = doPickInstance(instance0, instance1);

        // set nullability
        Boolean resultIsNullable = result.nullability();
        final Boolean resultDesiredNullability;
        Boolean leftIsNullable = instance0.nullability();
        if (leftIsNullable == null) {
            resultDesiredNullability = null;
        }
        else {
            Boolean rightIsNullable = instance0.nullability();
            resultDesiredNullability = (rightIsNullable == null)
                    ? null
                    : (leftIsNullable || rightIsNullable);
        }
        if (!Objects.equal(resultIsNullable, resultDesiredNullability)) {
            // need to set the nullability. But if the result was one of the inputs, need to defensively copy it first.
            if ( (result == instance0) || (result == instance1) )
                result = new TInstance(result);
            result.setNullable(resultDesiredNullability);
        }
        return result;
    }

    public PUnderlying underlyingType() {
        return pUnderlying;
    }

    public int nAttributes() {
        return attributes.length;
    }

    public String attributeName(int index) {
        return attributes[index].name();
    }

    public TName name() {
        return name;
    }

    public int internalRepresentationVersion() {
        return internalRepVersion;
    }

    public int serializationVersion() {
        return serializationVersion;
    }

    public boolean hasFixedSerializationSize() {
        return serializationSize >= 0;
    }

    public int fixedSerializationSize() {
        assert hasFixedSerializationSize() : this + " has no fixed serialization size";
        return serializationSize;
    }

    // object interface

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TClass other = (TClass) o;
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name.toString();
    }

    void format(TInstance instance, PValueSource source, AkibanAppender out) {
        if (source.isNull())
            out.append("NULL");
        else
            formatter.format(instance, source, out);
    }

    void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
        if (source.isNull())
            out.append("NULL");
        else
            formatter.formatAsLiteral(instance, source, out);
    }

    // for use by subclasses

    protected abstract TInstance doPickInstance(TInstance instance0, TInstance instance1);
        
    protected abstract void validate(TInstance instance);
    // for use by this class

    protected TInstance createInstanceNoArgs() {
        return createInstance(0, EMPTY, EMPTY, EMPTY, EMPTY);
    }

    protected TInstance createInstance(int nAttrs, int attr0, int attr1, int attr2, int attr3) {
        if (nAttributes() != nAttrs)
            throw new AkibanInternalException(name() + " requires " + nAttributes() + " attributes, saw " + nAttrs);
        TInstance result = new TInstance(this, enumClass, nAttrs, attr0, attr1, attr2, attr3);
        validate(result);
        return result;
    }

    public PValueCacher cacher() {
        return null;
    }

    // state

     protected <A extends Enum<A> & Attribute> TClass(TName name,
            Class<A> enumClass,
            TClassFormatter formatter,
            int internalRepVersion, int serializationVersion, int serializationSize,
            PUnderlying pUnderlying)
     {

         ArgumentValidation.notNull("name", name);
         this.name = name;
         this.formatter = formatter;
         this.internalRepVersion = internalRepVersion;
         this.serializationVersion = serializationVersion;
         this.serializationSize = serializationSize < 0 ? -1 : serializationSize; // normalize all negative numbers
         this.pUnderlying = pUnderlying;
         attributes = enumClass.getEnumConstants();

         this.enumClass = enumClass;
         for (int i = 0; i < attributes.length; ++i)
         {
             String attrValue = attributes[i].name();
             if (!VALID_ATTRIBUTE_PATTERN.matcher(attrValue).matches())
                 throw new IllegalNameException("attribute[" + i + "] for " + name + " has invalid name: " + attrValue);
         }
     }

     protected <A extends Enum<A> & Attribute> TClass(TBundleID bundle,
            String name,
            Enum<?> category,
            Class<A> enumClass,
            TClassFormatter formatter,
            int internalRepVersion, int serializationVersion, int serializationSize,
            PUnderlying pUnderlying)
     {
        this(new TName(bundle, name, category),
                enumClass,
                formatter,
                internalRepVersion, serializationVersion, serializationSize,
                pUnderlying);

     }

    public static void ensureRawValue(PValueSource source, TInstance instance) {
        if (!source.hasAnyValue())
            throw new IllegalStateException("no value set");
        if (!source.hasRawValue()) {
            PValueCacher cacher = instance.typeClass().cacher();
            if (cacher == null)
                throw new IllegalArgumentException("no cacher for " + instance + " with value " + source);
            if (source instanceof PValue) {
                ((PValue)source).ensureRaw(cacher, instance);
            }
            else
                throw new IllegalArgumentException("can't update value of type " + source.getClass());
        }
    }

    private final TName name;
    private final Class<?> enumClass;
    protected final TClassFormatter formatter;
    private final Attribute[] attributes;
    private final int internalRepVersion;
    private final int serializationVersion;
    private final int serializationSize;
    private final PUnderlying pUnderlying;

    private static final Pattern VALID_ATTRIBUTE_PATTERN = Pattern.compile("[a-zA-Z]\\w*");
    private static final int EMPTY = -1;
}
