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
package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCastPath;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public final class Cast_From_Varbinary {

    public static final TCast TO_VARCHAR = new TCastBase(MBinary.VARBINARY, MString.VARCHAR) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            String charset = DEFAULT_CHARSET;
            byte[] asBytes = source.getBytes();
            String asString;
            try {
                asString = new String(asBytes, DEFAULT_CHARSET);
            } catch (UnsupportedEncodingException e) {
                throw new AkibanInternalException("while decoding bytes to " + DEFAULT_CHARSET, e);
            }
            target.putString(asString, null);
        }
    };

    public static final TCast TO_LONGBLOB = new TCastBase(MBinary.VARBINARY, MBinary.LONGBLOB) {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBytes(source.getBytes());
        }
    };

    private static final String DEFAULT_CHARSET = Charset.defaultCharset().name();

    private Cast_From_Varbinary() {}
}
