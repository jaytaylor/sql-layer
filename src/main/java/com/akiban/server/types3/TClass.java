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
import com.akiban.util.ArgumentValidation;

import java.util.regex.Pattern;

public abstract class TClass {

    public abstract PUnderlying underlyingType();

    public TInstance combine(TCombineMode mode, TInstance instance0, TInstance instance1) {
        if (instance0.typeClass() != this || instance1.typeClass() != this)
            throw new IllegalArgumentException("can't combine " + instance0 + " and " + instance1 + " using " + this);
        return doCombine(mode, instance0, instance1);
    }

    public int nAttributes() {
        return attributes.length;
    }

    public String attributeName(int index) {
        return attributes[index];
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

    protected abstract TInstance doCombine(TCombineMode mode, TInstance instance0, TInstance instance1);

    protected TClass(TBundleID bundle, String name, String[] attributes, int internalRepVersion, int serializationVersion, int serializationSize) {
        this(new TName(bundle, name), attributes,  internalRepVersion, serializationVersion, serializationSize);
    }

    protected TClass(TName name, String[] attributes, int internalRepVersion, int serializationVersion, int serializationSize) {
        ArgumentValidation.notNull("name", name);
        this.name = name;
        this.internalRepVersion = internalRepVersion;
        this.serializationVersion = serializationVersion;
        this.serializationSize = serializationSize < 0 ? -1 : serializationSize; // normalize all negative numbers
        this.attributes = new String[attributes.length];
        for (int i = 0; i < attributes.length; ++i) {
            String attrValue = attributes[i];
            if (!ALL_ALPHAS.matcher(attrValue).matches())
                throw new IllegalNameException("attribute[" + i + "] for " + name + " has invalid name: " + attrValue);
            this.attributes[i] = attrValue;
        }
    }

    private final TName name;
    private final String[] attributes;
    private final int internalRepVersion;
    private final int serializationVersion;
    private final int serializationSize;

    private static final Pattern ALL_ALPHAS = Pattern.compile("[a-z][A-Z]+");

}
