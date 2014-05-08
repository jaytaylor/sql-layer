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

package com.foundationdb.server.types;

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.util.Strings;

import java.util.HashMap;
import java.util.Map;


public class FormatOptions {

    public interface FormatOption {
        public String format(byte[] bytes);
    }
    
    public static enum BinaryFormatOption implements FormatOption {
        OCTAL,
        HEX,
        BASE64;

        public static BinaryFormatOption fromProperty(String name) {
            try {
                return BinaryFormatOption.valueOf(name.toUpperCase());
            }
            catch(IllegalArgumentException iae) {
                throw new InvalidParameterValueException(name);
            }
        }

        public String format(byte[] bytes) {
            StringBuilder out = new StringBuilder();
            if (this.equals(FormatOptions.BinaryFormatOption.OCTAL)) {
                out.append(Strings.toOctal(bytes));
            } else if (this.equals(FormatOptions.BinaryFormatOption.HEX)) {
                out.append("\\x");
                out.append(Strings.hex(bytes));
            } else if (this.equals(FormatOptions.BinaryFormatOption.BASE64)) {
                out.append(Strings.toBase64(bytes));
            }
            return out.toString();
        }
    }

    public static enum JsonBinaryFormatOption implements FormatOption {
        OCTAL,
        HEX,
        BASE64;

        public static JsonBinaryFormatOption fromProperty(String name) {
            try {
                return JsonBinaryFormatOption.valueOf(name.toUpperCase());
            }
            catch(IllegalArgumentException iae) {
                throw new InvalidParameterValueException(name);
            }
        }
        
        public String format(byte[] bytes) {
            StringBuilder out = new StringBuilder();
            if (this.equals(FormatOptions.JsonBinaryFormatOption.OCTAL)) {
                out.append(Strings.toOctal(bytes));
            } else if (this.equals(FormatOptions.JsonBinaryFormatOption.HEX)) {
                out.append("\\x");
                out.append(Strings.hex(bytes));
            } else if (this.equals(FormatOptions.JsonBinaryFormatOption.BASE64)) {
                out.append(Strings.toBase64(bytes));
            }
            return out.toString();
        }
    }

    private final Map<Class<? extends FormatOption>, FormatOption> opts = new HashMap<>();

    public <T extends FormatOption> void set(T value) {
        opts.put(value.getClass(), value);
    }

    @SuppressWarnings("unchecked")
    public <T extends FormatOption> T get(Class<T> clazz) {
        return (T)opts.get(clazz);
    }
    
}
