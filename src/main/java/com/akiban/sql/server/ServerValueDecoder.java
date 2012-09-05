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

package com.akiban.sql.server;

import com.akiban.server.error.UnsupportedCharsetException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import java.io.UnsupportedEncodingException;

public class ServerValueDecoder
{
    private String encoding;
    private FromObjectValueSource objectSource;

    public ServerValueDecoder(String encoding) {
        this.encoding = encoding;
        objectSource = new FromObjectValueSource();
    }

    /** Decode the given value into a the given bindings at the given position.
     */
    public  <T extends ServerSession> void decodeValue(byte[] encoded, ServerType type, boolean binary,
                                                       ServerQueryContext<T> context, int index) {
        AkType akType = null;
        if (type != null)
            akType = type.getAkType();
        if (akType == null)
            akType = AkType.VARCHAR;
        Object value;
        if (binary || (encoded == null)) {
            value = encoded;
        }
        else {
            try {
                value = new String(encoded, encoding);
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException("", "", encoding);
            }
        }
        objectSource.setReflectively(value);
        context.setValue(index, objectSource, akType);
    }
   
    public <T extends ServerSession> void decodePValue(byte[] encoded, ServerType type, boolean binary,
                                                       ServerQueryContext<T> context, int index) {
        AkType akType = null;
        if (type != null)
            akType = type.getAkType();
        if (akType == null)
            akType = AkType.VARCHAR;
        Object value;
        if (binary || (encoded == null)) {
            value = encoded;
        }
        else {
            try {
                value = new String(encoded, encoding);
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException("", "", encoding);
            }
        }
        PValueSource source = PValueSources.fromObject(value, akType).value();
        context.setPValue(index, source);
    }

}
