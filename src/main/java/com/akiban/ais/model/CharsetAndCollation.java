package com.akiban.ais.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CharsetAndCollation implements Serializable
{
    // Required by GWT
    public CharsetAndCollation()
    {}
    
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

    private String charset;
    private String collation;
}
