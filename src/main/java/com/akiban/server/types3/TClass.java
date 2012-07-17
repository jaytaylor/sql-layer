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

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueCacher;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.util.ArgumentValidation;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.regex.Pattern;

public abstract class TClass {

    public abstract TFactory factory();

    public abstract DataTypeDescriptor dataTypeDescriptor(TInstance instance);

    public abstract void fromObject (TExecutionContext contextForErrors,
                            PValueSource in, TInstance outTInstance, PValueTarget out);

    public void selfCast(TExecutionContext context,
                         TInstance sourceInstance, PValueSource source, TInstance targetInstance, PValueTarget target) {
        target.putValueSource(source); // TODO make abstract
    }

    public int compare(TInstance instanceA, PValueSource sourceA, TInstance instanceB, PValueSource sourceB) {
        if (sourceA.isNull())
            return sourceB.isNull() ? 0 : -1;
        if (sourceB.isNull())
            return 1;

        if (sourceA.hasCacheValue() && sourceB.hasCacheValue()) {
            Object objectA = sourceA.getObject();
            if (objectA instanceof Comparable<?>) {
                // assume objectA and objectB are of the same class. If it's comparable, use that
                Comparable comparableA = (Comparable<?>) objectA;
                return comparableA.compareTo(sourceB.getObject());
            }
        }
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

    public void writeCanonical(PValueSource in, TInstance typeInstance, PValueTarget out) {
        PValueTargets.copyFrom(in, out, cacher(), typeInstance);
    }

    public void attributeToString(int attributeIndex, long value, StringBuilder output) {
        output.append(value);
    }

    public abstract void putSafety(QueryContext context,
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

    public void writeCollating(PValueSource inValue, TInstance inInstance, PValueTarget out) {
        writeCanonical(inValue, inInstance, out);
    }

    public void readCanonical(PValueSource inValue, TInstance typeInstance, PValueTarget out) {
        writeCanonical(inValue, typeInstance, out);
    }

    public TInstance pickInstance(TInstance instance0, TInstance instance1) {
        if (instance0.typeClass() != this || instance1.typeClass() != this)
            throw new IllegalArgumentException("can't combine " + instance0 + " and " + instance1 + " using " + this);
        return doPickInstance(instance0, instance1);
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

    public PValueCacher<?> cacher() {
        return null;
    }

    // state

     protected <A extends Enum<A> & Attribute> TClass(TName name,
            Class<A> enumClass,
            int internalRepVersion, int serializationVersion, int serializationSize,
            PUnderlying pUnderlying)
     {

         ArgumentValidation.notNull("name", name);
         this.name = name;
         this.internalRepVersion = internalRepVersion;
         this.serializationVersion = serializationVersion;
         this.serializationSize = serializationSize < 0 ? -1 : serializationSize; // normalize all negative numbers
         this.pUnderlying = pUnderlying;
         EnumSet<? extends Attribute> legalAttributes = EnumSet.allOf(enumClass);
         attributes = new Attribute[legalAttributes.size()];
         legalAttributes.toArray(attributes);

         this.enumClass = enumClass;
         for (int i = 0; i < attributes.length; ++i)
         {
             String attrValue = attributes[i].name();
             if (!ALL_ALPHAS.matcher(attrValue).matches())
                 throw new IllegalNameException("attribute[" + i + "] for " + name + " has invalid name: " + attrValue);
         }
     }

     protected <A extends Enum<A> & Attribute> TClass(TBundleID bundle,
            String name,
            Class<A> enumClass,
            int internalRepVersion, int serializationVersion, int serializationSize,
            PUnderlying pUnderlying)
     {
        this(new TName(bundle, name),
                enumClass,
                internalRepVersion, serializationVersion, serializationSize,
                pUnderlying);

     }

    private final TName name;
    private final Class<?> enumClass;
    private final Attribute[] attributes;
    private final int internalRepVersion;
    private final int serializationVersion;
    private final int serializationSize;
    private final PUnderlying pUnderlying;
    private static final Pattern ALL_ALPHAS = Pattern.compile("[a-zA-Z]+");
    private static final int EMPTY = -1;
}
