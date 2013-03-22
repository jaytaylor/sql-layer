
package com.akiban.server.types3.common.types;

import com.akiban.server.error.AkibanInternalException;

import java.util.HashMap;
import java.util.Map;

public class StringFactory
{
    //--------------------------------CHARSET-----------------------------------
    //TODO: add more charsets as needed
    public static enum Charset
    {
        LATIN1, UTF8, UTF16, ISO_8859_1
        ;
        
        public static Charset of(String value) {
            // Could optimize this with a StringBuilder, for-loop, etc
            value = value.toUpperCase();
            Charset charset = lookupMap.get(value);
            if (charset == null)
                throw new AkibanInternalException("not a valid encoding: " + value);
            return charset;
        }
        
        public static String of (int ordinal)
        {
            return Charset.values()[ordinal].name();
        }

        private static final Map<String,Charset> lookupMap = createLookupMap();

        private static Map<String, Charset> createLookupMap() {
            Map<String,Charset> map = new HashMap<>();
            for (Charset charset : Charset.values()) {
                map.put(charset.name(), charset);
            }
            // aliases
            map.put("ISO-8859-1", LATIN1);
            map.put("UTF-8", UTF8);
            map.put("UTF-16", UTF16);
            return map;
        }
    }
    
    //------------------------------Default values------------------------------
    
    // default number of characters in a string      
    protected static final int DEFAULT_LENGTH = 255;
    
    public static final Charset DEFAULT_CHARSET = Charset.UTF8;
    
    public static final int DEFAULT_COLLATION_ID = 0; // UCS_BINARY
    public static final int NULL_COLLATION_ID = -1; // String literals
    
    //--------------------------------------------------------------------------

    private StringFactory() {}
}