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

import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueCacher;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ArgumentValidation;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;

import java.util.regex.Pattern;

public abstract class TClass {

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
        if (leftClass.normalizeInstancesBeforeComparison())
            return !left.equalsExcludingNullable(right);
        else if (leftClass.getClass() == rightClass.getClass())
            return !leftClass.compatibleForCompare(rightClass);
        else
            return true;
    }

    public boolean compatibleForCompare(TClass other) {
        return (this == other);
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
                @SuppressWarnings("unchecked")
                Comparable<Object> comparableA = (Comparable<Object>) objectA;
                return comparableA.compareTo(sourceB.getObject());
            }
        }
        switch (TInstance.pUnderlying(sourceA.tInstance())) {
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
            return UnsignedBytes.lexicographicalComparator().compare(sourceA.getBytes(), sourceB.getBytes());
        case STRING:
            return sourceA.getString().compareTo(sourceB.getString());
        default:
            throw new AssertionError(sourceA.tInstance());
        }
    }

    final void writeCanonical(PValueSource in, TInstance typeInstance, PValueTarget out) {
        if (in.isNull())
            out.putNull();
        else
            getPValueIO().copyCanonical(in, typeInstance, out);
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

    protected PValueIO getPValueIO() {
        return defaultPValueIO;
    }

    public abstract TInstance instance(boolean nullable);

    public TInstance instance(int arg0, boolean nullable)
    {
        return createInstance(1, arg0, EMPTY, EMPTY, EMPTY, nullable);
    }

    public TInstance instance(int arg0, int arg1, boolean nullable)
    {
        return createInstance(2, arg0, arg1, EMPTY, EMPTY, nullable);
    }

    public TInstance instance(int arg0, int arg1, int arg2, boolean nullable)
    {
        return createInstance(3, arg0, arg1, arg2, EMPTY, nullable);
    }

    public TInstance instance(int arg0, int arg1, int arg2, int arg3, boolean nullable)
    {
        return createInstance(4, arg0, arg1, arg2, arg3, nullable);
    }

    final void writeCollating(PValueSource inValue, TInstance inInstance, PValueTarget out) {
        if (inValue.isNull())
            out.putNull();
        else
            getPValueIO().writeCollating(inValue, inInstance, out);
    }

    final void readCollating(PValueSource inValue, TInstance inInstance, PValueTarget out) {
        if (inValue.isNull())
            out.putNull();
        else
            getPValueIO().readCollating(inValue, inInstance, out);
    }

    public TInstance pickInstance(TInstance left, TInstance right) {
        if (left.typeClass() != TClass.this || right.typeClass() != TClass.this)
            throw new IllegalArgumentException("can't combine " + left + " and " + right + " using " + this);

        return doPickInstance(left, right, left.nullability() || right.nullability());
    }

    public PUnderlying underlyingType() {
        return pUnderlying;
    }

    public int nAttributes() {
        return attributes.length;
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

    void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
        if (source.isNull())
            out.append("null");
        else
            formatter.formatAsJson(instance, source, out);
    }

    public Object formatCachedForNiceRow(PValueSource source) {
        return source.getObject();
    }

    // for use by subclasses
    protected abstract TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability);
    protected abstract void validate(TInstance instance);

    // for use by this class

    protected TInstance createInstanceNoArgs(boolean nullable) {
        return createInstance(0, EMPTY, EMPTY, EMPTY, EMPTY, nullable);
    }

    protected TInstance createInstance(int nAttrs, int attr0, int attr1, int attr2, int attr3, boolean nullable) {
        return TInstance.create(this, enumClass, nAttrs, attr0, attr1, attr2, attr3, nullable);
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

    private static final PValueIO defaultPValueIO = new SimplePValueIO() {
        @Override
        protected void copy(PValueSource in, TInstance typeInstance, PValueTarget out) {
            PValueTargets.copyFrom(in, out);
        }
    };
}
