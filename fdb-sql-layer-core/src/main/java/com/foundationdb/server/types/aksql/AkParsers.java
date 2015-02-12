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

package com.foundationdb.server.types.aksql;

import com.foundationdb.server.error.InvalidGuidFormatException;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

public class AkParsers
{
    public static final TParser BOOLEAN = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            String s = source.getString();
            boolean result = false;
            if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("t")) {
                result = true;
            }
            else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("f")) {
                result = false;
            }
            else {
                // parse source is a string representing a number-ish, where '0' is false, any other integer is true.
                // We're looking for an optional negative, followed by an optional dot, followed by any number of digits,
                // followed by anything. If any of those digits is not 0, the result is true; otherwise it's false.
                boolean negativeAllowed = true;
                boolean periodAllowed = true;
                for (int i = 0, len = s.length(); i < len; ++i) {
                    char c = s.charAt(i);
                    if (negativeAllowed && c == '-') {
                        negativeAllowed = false;
                    }
                    else if (periodAllowed && c == '.') {
                        periodAllowed = false;
                        negativeAllowed = false;
                    }
                    else if (Character.isDigit(c)) {
                        if (c != '0') {
                            result = true;
                            break;
                        }
                    }
                    else {
                        break;
                    }
                }
            }
            target.putBool(result);
        }
    };
    
    public static final TParser GUID = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target) {
            String s = source.getString();
            if (s.startsWith("{") && s.endsWith("}")) {
                s = s.substring(1, s.length()-1);
            }
            try {
                UUID uuid = UUID.fromString(s);
                target.putObject(uuid);
            } catch (IllegalArgumentException e) {
                throw new InvalidGuidFormatException(s);
            }
        }
    };

    public static final TParser BLOB = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target) {
            String s = source.getString();
            int charsetId = source.getType().attribute(StringAttribute.CHARSET);
            String charsetName = StringFactory.Charset.values()[charsetId].name();
            byte[] bytes;
            try {
                bytes = s.getBytes(charsetName);
            } catch (UnsupportedEncodingException e) {
                throw new AkibanInternalException("while decoding string using " + charsetName, e);
            }
            BlobRef blob = new BlobRef(bytes);
            target.putObject(blob);
        }
    };
}
