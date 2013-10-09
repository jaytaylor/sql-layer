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
package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.IllegalNameException;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.NumericFormatter;
import com.foundationdb.server.types.common.types.SimpleDtdTClass;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.pvalue.PUnderlying;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.pvalue.PValueSources;
import com.foundationdb.server.types.pvalue.PValueTarget;
import com.foundationdb.server.types.texpressions.Serialization;
import com.foundationdb.server.types.texpressions.SerializeAs;
import com.foundationdb.sql.types.TypeId;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public final class MBinary extends SimpleDtdTClass {

    private static final int MAX_BYTE_BUF = 4096;
    private static final TParser parser = new BinaryParser();

    public static final TClass VARBINARY = new MBinary(TypeId.VARBIT_ID, "varbinary", -1);
    public static final TClass BINARY = new MBinary(TypeId.BIT_ID, "binary", -1);
    public static final TClass TINYBLOB = new MBinary(TypeId.BLOB_ID, "tinyblob", 256);
    public static final TClass MEDIUMBLOB = new MBinary(TypeId.BLOB_ID, "mediumblob", 65535);
    public static final TClass BLOB = new MBinary(TypeId.BLOB_ID, "blob", 16777215);
    public static final TClass LONGBLOB = new MBinary(TypeId.BLOB_ID, "longblob", Integer.MAX_VALUE); // TODO not big enough!
    
    public enum Attrs implements Attribute {
        @SerializeAs(Serialization.LONG_1) LENGTH
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        return false;
    }

    public TClass widestComparable()
    {
        return this;
    }
    
    @Override
    public void fromObject(TExecutionContext context, PValueSource in, PValueTarget out)
    {
        if (in.isNull()) {
            out.putNull();
            return;
        }
        
        byte[] bytes;
        PUnderlying underlying = PValueSources.pUnderlying(in);
        if (underlying == PUnderlying.BYTES) {
            bytes = in.getBytes();
        }
        else if (underlying == PUnderlying.STRING) {
            try {
                bytes = in.getString().getBytes("utf8");
            } catch (UnsupportedEncodingException e) {
                throw new AkibanInternalException("while converting to bytes: " + in.getString(), e);
            }
        }
        else {
            throw new AkibanInternalException("couldn't convert to byte[]: " + in);
        }

        int expectedLength = context.outputTInstance().attribute(Attrs.LENGTH);
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
    protected void validate(TInstance instance) {
        int len = instance.attribute(Attrs.LENGTH);
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

    private MBinary(TypeId typeId, String name, int defaultLength) {
        super(MBundle.INSTANCE.id(), name, AkCategory.STRING_BINARY, NumericFormatter.FORMAT.BYTES, Attrs.class,
                1, 1, -1, PUnderlying.BYTES, parser, (defaultLength < 0 ? MAX_BYTE_BUF : defaultLength), typeId);
        this.defaultLength = defaultLength;
    }

    private final int defaultLength;

    public static void putBytes(TExecutionContext context, PValueTarget target, byte[] bytes) {
        int maxLen = context.outputTInstance().attribute(MBinary.Attrs.LENGTH);
        if (bytes.length > maxLen) {
            context.reportTruncate("bytes of length " + bytes.length,  "bytes of length " + maxLen);
            bytes = Arrays.copyOf(bytes, maxLen);
        }
        target.putBytes(bytes);
    }

    private static class BinaryParser implements TParser {
        @Override
        public void parse(TExecutionContext context, PValueSource in, PValueTarget out) {
            String string = in.getString();
            int charsetId = context.inputTInstanceAt(0).attribute(StringAttribute.CHARSET);
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
