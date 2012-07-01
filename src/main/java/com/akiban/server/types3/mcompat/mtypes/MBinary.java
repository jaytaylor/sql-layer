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

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.IllegalNameException;
import com.akiban.server.types3.TAttributeValues;
import com.akiban.server.types3.TAttributesDeclaration;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public final class MBinary extends TClass {

    public static final TClass VARBINARY = new MBinary("varbinary", -1);
    public static final TClass BINARY = new MBinary("varbinary", -1);
    public static final TClass TINYBLOB = new MBinary("tinyblob", 256);
    public static final TClass MEDIUMBLOB = new MBinary("mediumblob", 65535);
    public static final TClass BLOB = new MBinary("blob", 16777215);
    public static final TClass LONGBLOB = new MBinary("longblob", Integer.MAX_VALUE); // TODO not big enough!
    
    public enum Attrs implements Attribute {
        LENGTH
    }

    @Override
    public TFactory factory() {
        return new TFactory() {
            @Override
            public TInstance create(TAttributesDeclaration declaration) {
                final int len;
                if (defaultLength < 0) {
                    TAttributeValues values = declaration.validate(1, 1);
                    len = values.intAt(Attrs.LENGTH, defaultLength);
                    if (len < 0)
                        throw new IllegalNameException("length must be positive");
                }
                else {
                    declaration.validate(0, 0);
                    len = defaultLength;
                }
                return instance(len);
            }
        };
    }

    @Override
    public void putSafety(QueryContext context, TInstance sourceInstance, PValueSource sourceValue,
                          TInstance targetInstance, PValueTarget targetValue) {
        targetValue.putBytes(sourceValue.getBytes());
    }

    @Override
    public TInstance instance() {
        return instance(defaultLength);
    }

    @Override
    protected TInstance doPickInstance(TInstance instance0, TInstance instance1) {
        int len0 = instance0.attribute(Attrs.LENGTH);
        int len1 = instance0.attribute(Attrs.LENGTH);
        return len0 > len1 ? instance0 : instance1;
    }

    @Override
    protected void validate(TInstance instance) {
        int len = instance.attribute(Attrs.LENGTH);
        if (defaultLength < 0) {
            if (len < 0)
                throw new IllegalNameException("length must be positive");
        }
        else {
            assert len == defaultLength : "expected length=" + defaultLength + " but was " + len;
        }
    }
    
    private MBinary(String name, int defaultLength) {
        super(MBundle.INSTANCE.id(), name, Attrs.class, 1, 1, -1, PUnderlying.BYTES);
        this.defaultLength = defaultLength;
    }
    
    private final int defaultLength;
}
