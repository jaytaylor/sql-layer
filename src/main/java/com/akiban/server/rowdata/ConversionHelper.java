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

package com.akiban.server.rowdata;

import com.akiban.ais.model.TableName;
import com.akiban.server.AkServerUtil;
import com.akiban.server.error.UnsupportedCharsetException;

import java.io.UnsupportedEncodingException;

final class ConversionHelper {
    // "public" methods

    /**
     * Writes a VARCHAR or CHAR: inserts the correct-sized PREFIX for MySQL
     * VARCHAR. Assumes US-ASCII encoding, for now. Can be used temporarily for
     * the BLOB types as well.
     *
     * @param string Othe string to put in
     * @param bytes Buffer to write bytes into
     * @param offset Position within bytes to write at
     * @param fieldDef Corresponding field
     * @return How many positions in bytes were used
     */
    public static int encodeString(String string, final byte[] bytes, final int offset, final FieldDef fieldDef) {
        assert string != null;
        final byte[] b;
        String charsetName = fieldDef.column().getCharsetAndCollation().charset();
        try {
            b = string.getBytes(charsetName);
        } catch (UnsupportedEncodingException e) {
            TableName table = fieldDef.column().getTable().getName();
            throw new UnsupportedCharsetException(table.getSchemaName(), table.getTableName(), charsetName);
        }
        return putByteArray(b, 0, b.length, bytes, offset, fieldDef);
    }

    public static int putByteArray(final byte[] src, final int srcOffset, final int srcLength,
                                   final byte[] dst, final int dstOffset, final FieldDef fieldDef) {
        int prefixSize = fieldDef.getPrefixSize();
        AkServerUtil.putIntegerByWidth(dst, dstOffset, prefixSize, srcLength);
        System.arraycopy(src, srcOffset, dst, dstOffset + prefixSize, srcLength);
        return prefixSize + srcLength;

    }

    // for use in this class

    private ConversionHelper() {}
}
