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

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.IllegalNameException;
import com.foundationdb.server.types.TBundle;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TClassBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.NumericFormatter;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Serialization;
import com.foundationdb.server.types.texpressions.SerializeAs;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import java.io.UnsupportedEncodingException;
import java.sql.Types;
import java.util.Arrays;

public abstract class TBinary extends TClassBase {

    protected static final int MAX_BYTE_BUF = 4096;
    protected static final TParser parser = new BinaryParser();
    
    public enum Attrs implements Attribute {
        @SerializeAs(Serialization.LONG_1) LENGTH
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        return false;
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        return (defaultLength < 0);
    }

    public TClass widestComparable()
    {
        return this;
    }
    
    @Override
    public void fromObject(TExecutionContext context, ValueSource in, ValueTarget out)
    {
        if (in.isNull()) {
            out.putNull();
            return;
        }
        
        byte[] bytes;
        UnderlyingType underlying = ValueSources.underlyingType(in);
        if (underlying == UnderlyingType.BYTES) {
            bytes = in.getBytes();
        }
        else if (underlying == UnderlyingType.STRING) {
            try {
                bytes = in.getString().getBytes("utf8");
            } catch (UnsupportedEncodingException e) {
                throw new AkibanInternalException("while converting to bytes: " + in.getString(), e);
            }
        }
        else {
            throw new AkibanInternalException("couldn't convert to byte[]: " + in);
        }

        int expectedLength = context.outputType().attribute(Attrs.LENGTH);
        if (bytes.length > expectedLength)
        {
            out.putBytes(Arrays.copyOf(bytes, expectedLength));
            context.reportTruncate("BINARY string of LENGTH: " + bytes.length,
                                   "BINARY string of LENGTH: " + expectedLength);
        }
        else
            out.putBytes(bytes);
    }

    @Override
    public TInstance instance(boolean nullable) {
        // 'defaultLength' doesn't always mean "LENGTH"
        // -1 simply means a (VAR)BINARY type, in which case, you don't want
        // to create an instance with length -1, but with MAX_BYTE_BUF (4096)
        return instance(defaultLength < 0 ? MAX_BYTE_BUF : defaultLength, nullable);
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        int len0 = left.attribute(Attrs.LENGTH);
        int len1 = left.attribute(Attrs.LENGTH);
        return len0 > len1 ? left : right;
    }

    @Override
    protected void validate(TInstance type) {
        int len = type.attribute(Attrs.LENGTH);
        if (defaultLength < 0) {
            // This is BINARY or VARBINARY, so the user set the length
            if (len < 0)
                throw new IllegalNameException("length must be non-negative");
        }
        else {
            // This is one of the blob types, so the length has to be exactly what we expect
            assert len == defaultLength : "expected length=" + defaultLength + " but was " + len;
        }
    }

    protected TBinary(TypeId typeId, TBundle bundle, String name, int defaultLength) {
        super(bundle.id(), name, AkCategory.STRING_BINARY, Attrs.class, NumericFormatter.FORMAT.BYTES,
                1, 1, -1, UnderlyingType.BYTES, parser, (defaultLength < 0 ? MAX_BYTE_BUF : defaultLength));
        this.typeId = typeId;
        this.defaultLength = defaultLength;
    }

    private final TypeId typeId;
    private final int defaultLength;

    public int getDefaultLength() {
        return defaultLength;
    }

    @Override
    public int jdbcType() {
        if (defaultLength < 0)
            return typeId.getJDBCTypeId(); // [VAR]BINARY
        else
            return Types.LONGVARBINARY; // Not BLOB.
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance type) {
        return new DataTypeDescriptor(typeId,
                                      type.nullability(),
                                      type.attribute(Attrs.LENGTH));
    }

    @Override
    public int variableSerializationSize(TInstance type, boolean average) {
        return type.attribute(Attrs.LENGTH);
    }

    public static void putBytes(TExecutionContext context, ValueTarget target, byte[] bytes) {
        int maxLen = context.outputType().attribute(Attrs.LENGTH);
        if (bytes.length > maxLen) {
            context.reportTruncate("bytes of length " + bytes.length,  "bytes of length " + maxLen);
            bytes = Arrays.copyOf(bytes, maxLen);
        }
        else if ((bytes.length < maxLen) &&
                 (((TBinary)context.outputType().typeClass()).typeId == TypeId.BIT_ID)) {
            bytes = Arrays.copyOf(bytes, maxLen);
        }
        target.putBytes(bytes);
    }

    private static class BinaryParser implements TParser {
        @Override
        public void parse(TExecutionContext context, ValueSource in, ValueTarget out) {
            String string = in.getString();
            int charsetId = context.inputTypeAt(0).attribute(StringAttribute.CHARSET);
            String charsetName = StringFactory.Charset.values()[charsetId].name();
            byte[] bytes;
            try {
                bytes = string.getBytes(charsetName);
            } catch (UnsupportedEncodingException e) {
                throw new AkibanInternalException("while decoding string using " + charsetName, e);
            }
            putBytes(context, out, bytes);
        }
    }
}
