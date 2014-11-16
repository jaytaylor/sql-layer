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

package com.foundationdb.server.types;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.util.AkibanAppender;

import java.util.ArrayList;
import java.util.List;

/**
 * A type including nullability and various other attributes (such as collation).
 *
 * Note:
 * A null TInstance can represent one of 4 things:
 * * A literal NULL, e.g. (SELECT NULL)
 * * A Parameter, e.g. (SELECT ?)
 * * An unkown return value, presumably due to one of the two above
 * * Temporarily unset type until it has been figured out.
 */
public final class TInstance {

    // static helpers

    public static TClass tClass(TInstance type) {
        return type == null ? null : type.typeClass();
    }

    public static UnderlyingType underlyingType(TInstance type) {
        TClass tClass = tClass(type);  
        return tClass == null ? null : tClass.underlyingType();
    }

    // TInstance interface

    public void writeCanonical(ValueSource in, ValueTarget out) {
        tclass.writeCanonical(in, this, out);
    }

    public void writeCollating(ValueSource in, ValueTarget out) {
        tclass.writeCollating(in, this, out);
    }

    public void readCollating(Value in, Value out) {
        tclass.readCollating(in, this, out);
    }

    public void format(ValueSource source, AkibanAppender out) {
        tclass.format(this, source, out);
    }

    public void formatAsLiteral(ValueSource source, AkibanAppender out) {
        tclass.formatAsLiteral(this, source, out);
    }

    public void formatAsJson(ValueSource source, AkibanAppender out, FormatOptions options) {
        tclass.formatAsJson(this, source, out, options);
    }

    public Object attributeToObject(Attribute attribute) {
        if (enumClass != attribute.getClass())
            throw new IllegalArgumentException("Illegal attribute: " + attribute.name());
        int value;
        int attributeIndex = attribute.ordinal();
        switch (attributeIndex) {
        case 0: value = attr0; break;
        case 1: value = attr1; break;
        case 2: value = attr2; break;
        case 3: value = attr3; break;
        default: throw new AssertionError("index out of range for " + tclass +  ": " + attribute);
        }
        return tclass.attributeToObject(attributeIndex, value);
    }

    public int attribute(Attribute attribute) {
        if (enumClass != attribute.getClass())
            throw new IllegalArgumentException("Illegal attribute: " + attribute.name());
        int index = attribute.ordinal();
        switch (index) {
        case 0: return attr0;
        case 1: return attr1;
        case 2: return attr2;
        case 3: return attr3;
        default: throw new IllegalArgumentException("index out of range for " + tclass +  ": " + index);
        }
    }

    public boolean hasAttributes(Class<?> enumClass) {
        return (this.enumClass == enumClass);
    }

    public TClass typeClass() {
        return tclass;
    }

    public boolean nullability() {
        return isNullable;
    }

    public Object getMetaData() {
        return metaData;
    }

    public TInstance withNullable(boolean isNullable) {
        return (isNullable == this.isNullable)
                ? this
                : new TInstance(tclass, enumClass, tclass.nAttributes(), attr0, attr1, attr2, attr3, isNullable);
    }
    /**
     * Convenience method for <tt>typeClass().dataTypeDescriptor(this)</tt>.
     * @return this instance's DataTypeDescriptor
     * @see TClass#dataTypeDescriptor(TInstance)
     */
    public DataTypeDescriptor dataTypeDescriptor() {
        return tclass.dataTypeDescriptor(this);
    }

    /**
     * @param o additional meta data for this TInstance
     * @return
     * <code>false</code> if this method has already been called on this object.
     * The new meta data will <e>not</e> override the current one.
     * <code>true</code> if this object's meta data is still <code>null</code>.
     */
    public boolean setMetaData (Object o) {
        if (metaData != null)
            return false;
        metaData = o;
        return true;
    }

    public String toStringIgnoringNullability(boolean useShorthand) {
        String className = useShorthand ? tclass.name().unqualifiedName() : tclass.name().toString();
        int nattrs = tclass.nAttributes();
        if (nattrs == 0)
            return className;
        // assume 5 digits per attribute as a wild guess. If it's wrong, no biggie. 2 chars for open/close paren
        int capacity = className.length() + 2 + (5*nattrs);
        StringBuilder sb = new StringBuilder(capacity);
        sb.append(className).append('(');
        long[] attrs = new long[] { attr0, attr1, attr2, attr3 };
        for (int i = 0; i < nattrs; ++i) {
            tclass.attributeToString(i, attrs[i], sb);
            if (i+1 < nattrs)
                sb.append(", ");
        }
        sb.append(')');
        return sb.toString();
    }

    public String toStringConcise(boolean lowerCase) {
        String name = tclass.name().unqualifiedName();
        if (lowerCase)
            name = name.toLowerCase();
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        int nattrs = tclass.nAttributes();
        boolean first = true;
        long[] attrs = new long[] { attr0, attr1, attr2, attr3 };
        for (int i = 0; i < nattrs; ++i) {
            if (tclass.attributeAlwaysDisplayed(i)) {
                if (first) {
                    sb.append("(");
                    first = false;
                }
                else {
                    sb.append(", ");
                }
                tclass.attributeToString(i, attrs[i], sb);
            }
        }
        if (!first) {
            sb.append(")");
        }
        return sb.toString();
    }

    // object interface

    @Override
    public String toString() {
        String result = toStringIgnoringNullability(false);
        if (tclass.nAttributes() != 0) // TODO there's no reason to do this except that it's backwards-compatible
            result += (isNullable ? " NULL" : " NOT NULL"); // with existing tests.
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return equalsIncludingNullable(o);
    }

    public boolean equalsIncludingNullable(Object o) {
        return equals(o, true);
    }

    public boolean equalsExcludingNullable(Object o) {
        return equals(o, false);
    }

    private boolean equals(Object o, boolean withNullable) {
        if (this == o) return true;
        if (!(o instanceof TInstance)) return false;

        TInstance other = (TInstance) o;

        return attr0 == other.attr0
                && attr1 == other.attr1
                && attr2 == other.attr2
                && attr3 == other.attr3
                && ((!withNullable) || (isNullable == other.isNullable))
                && tclass.equals(other.tclass);
    }

    @Override
    public int hashCode() {
        int result = tclass.hashCode();
        result = 31 * result + attr0;
        result = 31 * result + attr2;
        result = 31 * result + attr1;
        result = 31 * result + attr3;
        result = 31 * result + (isNullable ? 0 : 1);
        return result;
    }

    // package-private

    static TInstance create(TClass tclass, Class<?> enumClass, int nAttrs, int attr0, int attr1, int attr2, int attr3,
                            boolean isNullable)
    {
        TInstance result = new TInstance(tclass, enumClass, nAttrs, attr0, attr1, attr2, attr3, isNullable);
        tclass.validate(result);
        return result;
    }

    static TInstance create(TInstance template, int attr0, int attr1, int attr2, int attr3, boolean nullable) {
        return create(template.tclass, template.enumClass, template.tclass.nAttributes(),
                attr0, attr1, attr2, attr3, nullable);
    }

    Class<?> enumClass() {
        return enumClass;
    }

    int attrByPos(int i) {
        switch (i) {
        case 0: return attr0;
        case 1: return attr1;
        case 2: return attr2;
        case 3: return attr3;
        default: throw new AssertionError("out of range: " + i);
        }
    }

    // state
    private TInstance(TClass tclass, Class<?> enumClass, int nAttrs, int attr0, int attr1, int attr2, int attr3,
              boolean isNullable)
    {
        if (tclass.nAttributes() != nAttrs) {
            throw new AkibanInternalException(tclass.name() + " requires "+ tclass.nAttributes()
                    + " attributes, saw " + nAttrs);
        }
        // normalize inputs past nattrs
        switch (nAttrs) {
        case 0:
            attr0 = -1;
        case 1:
            attr1 = -1;
        case 2:
            attr2 = -1;
        case 3:
            attr3 = -1;
        case 4:
            break;
        default:
            throw new IllegalArgumentException("too many nattrs: " + nAttrs + " (" + enumClass.getSimpleName() + ')');
        }
        this.tclass = tclass;
        this.attr0 = attr0;
        this.attr1 = attr1;
        this.attr2 = attr2;
        this.attr3 = attr3;
        this.enumClass = enumClass;
        this.isNullable = isNullable;
    }
    private final TClass tclass;
    private final int attr0, attr1, attr2, attr3;
    private final boolean isNullable;

    private Object metaData;

    private final Class<?> enumClass;
}
