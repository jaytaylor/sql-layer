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
package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.IllegalNameException;
import com.akiban.server.types3.TAttributeValues;
import com.akiban.server.types3.TAttributesDeclaration;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TParser;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.common.NumericFormatter;
import com.akiban.server.types3.common.types.SimpleDtdTClass;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.common.types.StringFactory;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.sql.types.TypeId;

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
        LENGTH
    }

    public int getFixedLength() {
        return defaultLength;
    }

    @Override
    public void fromObject(TExecutionContext context, PValueSource in, PValueTarget out)
    {
        if (in.isNull()) {
            out.putNull();
            return;
        }
        
        byte[] bytes;
        PUnderlying underlying = in.getUnderlyingType();
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
    public void putSafety(TExecutionContext context, TInstance sourceInstance, PValueSource sourceValue,
                          TInstance targetInstance, PValueTarget targetValue) {
        targetValue.putBytes(sourceValue.getBytes());
    }

    @Override
    public TInstance instance() {
        // 'defaultLength' doesn't always mean "LENGTH"
        // -1 simply means a (VAR)BINARY type, in which case, you don't want
        // to create an instance with length -1, but with MAX_BYTE_BUF (4096)
        return instance(defaultLength < 0 ? MAX_BYTE_BUF : defaultLength);
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right) {
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
