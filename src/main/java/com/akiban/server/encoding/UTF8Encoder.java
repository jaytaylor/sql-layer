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

package com.akiban.server.encoding;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.rowdata.FieldDef;

import java.io.UnsupportedEncodingException;

/** Single byte encoding. */
public class UTF8Encoder extends VariableWidthEncoding {

    public static final Encoding INSTANCE = new UTF8Encoder();

    // See https://tools.ietf.org/html/rfc3629
    private static final int MAX_1_BYTE = 0x007F;
    private static final int MAX_2_BYTE = 0x07FF;
    private static final int MAX_3_BYTE = 0xFFFF;
    private static final int MAX_4_BYTE = 0x10FFFF;

    private UTF8Encoder() {
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int size = fieldDef.getPrefixSize();
        if (value != null) {
            String str;
            if (value instanceof byte[]) {
                try {
                    str = new String((byte[]) value, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new AkibanInternalException("while decoding binary", e);
                }
            }
            else {
                str = value.toString();
            }
            for (int i = 0; i < str.length(); i++) {
                int ch = str.charAt(i);
                // Assumes consumers want standard UTF8 (e.g. String, nio.charset), not modified
                if (ch <= MAX_1_BYTE)
                    size += 1;
                else if (ch <= MAX_2_BYTE)
                    size += 2;
                else {
                    // codePointAt will return the same as charAt if not a high surrogate pair *or* not followed by low
                    int codePoint = str.codePointAt(i);
                    if (codePoint == ch) {
                        size += 3;
                    } else {
                        if (++i >= str.length())
                            throw new IllegalStateException("Got codePoint but missing low pair: " + str);
                        if (codePoint <= MAX_3_BYTE)
                            size += 3;
                        else {
                            assert codePoint <= MAX_4_BYTE : "Illegal code point: " + codePoint;
                            size += 4;
                        }
                    }
                }
            }
        }
        return size;
    }
}
