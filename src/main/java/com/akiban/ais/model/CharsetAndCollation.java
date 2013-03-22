
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
    private static final Map<String, CharsetAndCollation> extent = new HashMap<>();

    private final String charset;
    private final String collation;
}
