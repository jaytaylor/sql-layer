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

package com.akiban.ais.model;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.collation.AkCollatorFactory;

import java.util.HashMap;
import java.util.Map;

public class CharsetAndCollation
{
    public static CharsetAndCollation intern(String charset, String collation)
    {
        String key = key(charset, collation);
        CharsetAndCollation charsetAndCollation = extent.get(key);
        if (charsetAndCollation == null) {
            charsetAndCollation = new CharsetAndCollation(charset, collation);
            extent.put(key, charsetAndCollation);
        }
        return charsetAndCollation;
    }

    public String charset()
    {
        return charset;
    }

    public String collation()
    {
        return collation;
    }

    // TODO It may be worth caching this here or inside cac,
    // once it is thread-safe.
    public AkCollator getCollator() {
        return AkCollatorFactory.getAkCollator(collation);
    }
    
    @Override
    public String toString() {
        return key(charset, collation);
    }

    private CharsetAndCollation(String charset, String collation)
    {
        this.charset = charset;
        this.collation = collation;
    }

    private static String key(String charset, String collation)
    {
        return charset + "/" + collation;
    }

    // charset/collation -> CharsetAndCollation
    private static final Map<String, CharsetAndCollation> extent = new HashMap<String, CharsetAndCollation>();

    private final String charset;
    private final String collation;
}
