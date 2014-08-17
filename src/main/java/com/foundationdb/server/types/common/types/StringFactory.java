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

package com.foundationdb.server.types.common.types;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.UnsupportedCharsetException;

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
                throw new UnsupportedCharsetException(value);
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
    
    public static int charsetNameToId(String name) {
        return (name == null) ? NULL_CHARSET_ID : Charset.of(name).ordinal();
    }

    public static String charsetIdToName(int id) {
        return (id == NULL_CHARSET_ID) ? null : Charset.of(id);
    }

    public static int collationNameToId(String name) {
        return (name == null) ? NULL_COLLATION_ID : AkCollatorFactory.getAkCollator(name).getCollationId();
    }

    public static String collationIdToName(int id) {
        return (id == NULL_COLLATION_ID) ? null : AkCollatorFactory.getAkCollator(id).getScheme();
    }

    //------------------------------Default values------------------------------
    
    // default number of characters in a string      
    protected static final int DEFAULT_LENGTH = 255;
    
    public static final Charset DEFAULT_CHARSET = Charset.UTF8;
    public static final int DEFAULT_CHARSET_ID = DEFAULT_CHARSET.ordinal();
    public static final int NULL_CHARSET_ID = -1;
    
    public static final int DEFAULT_COLLATION_ID = 0; // UCS_BINARY
    public static final int NULL_COLLATION_ID = -1; // String literals
    
    //--------------------------------------------------------------------------

    private StringFactory() {}
}
