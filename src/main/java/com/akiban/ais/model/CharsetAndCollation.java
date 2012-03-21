/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.model;

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
