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

package com.akiban.server.encoding;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.model.Type;

@SuppressWarnings("unused")
public final class EncoderFactory {
    private EncoderFactory() {
    }

    public static final IntEncoder INT = new IntEncoder();
    public static final UIntEncoder U_INT = new UIntEncoder();
    public static final UBigIntEncoder U_BIGINT = new UBigIntEncoder();
    public static final FloatEncoder FLOAT = new FloatEncoder();
    public static final UFloatEncoder U_FLOAT = new UFloatEncoder();
    public static final DoubleEncoder DOUBLE = new DoubleEncoder();
    public static final UDoubleEncoder U_DOUBLE = new UDoubleEncoder();
    public static final DecimalEncoder DECIMAL = new DecimalEncoder();
    public static final DecimalEncoder U_DECIMAL = new DecimalEncoder();
    public static final StringEncoder VARCHAR = new StringEncoder();
    public static final VarBinaryEncoder VARBINARY = new VarBinaryEncoder();
    public static final TextEncoder BLOB = new TextEncoder();  // TODO - temporarily we handle just like TEXT
    public static final TextEncoder TEXT = new TextEncoder();
    public static final DateEncoder DATE = new DateEncoder();
    public static final TimeEncoder TIME = new TimeEncoder();
    public static final DateTimeEncoder DATETIME = new DateTimeEncoder();
    public static final TimestampEncoder TIMESTAMP = new TimestampEncoder();
    public static final YearEncoder YEAR = new YearEncoder();

    private static final Object ENCODING_MAP_LOCK = EncoderFactory.class;
    private static Map<String,Encoding> encodingMap = null;

    /**
     * Gets an encoding by name.
     * @param name the encoding's name
     * @return the encoding
     * @throws EncodingException if no such encoding exists
     */
    private static Encoding valueOf(String name) {
        synchronized (ENCODING_MAP_LOCK) {
            if (encodingMap == null) {
                encodingMap = initializeEncodingMap();
            }
            final Encoding encoding = encodingMap.get(name);
            if (encoding == null) {
                throw new EncodingException("Unknown encoding type: " + name);
            }
            return encoding;
        }
    }

    private static Map<String, Encoding> initializeEncodingMap() {
        final Map<String,Encoding> tmp = new HashMap<String, Encoding>();
        for (Field field : EncoderFactory.class.getDeclaredFields()) {
            final int m = field.getModifiers();
            if (Modifier.isFinal(m) && Modifier.isStatic(m) && Modifier.isPublic(m) && Encoding.class.isAssignableFrom(field.getType())) {
                try {
                    Encoding encoding = (Encoding) field.get(null);
                    tmp.put(field.getName(), encoding);
                } catch (IllegalAccessException e) {
                    throw new EncodingException("While constructing encodings map; at " + field.getName(), e);
                }
            }
        }
        return Collections.unmodifiableMap(tmp);
    }

    /**
     * Gets an encoding by name, also verifying that it's valid for the given type
     * @param name the encoding's name
     * @param type the type to verify
     * @return the encoding
     * @throws EncodingException if the type is invalid for this encoding, or if the encoding doesn't exist
     */
    public static Encoding valueOf(String name, Type type) {
        Encoding encoding = valueOf(name);

        if (!encoding.validate(type)) {
            throw new EncodingException("Encoding " + encoding + " not valid for type " + type);
        }
        return encoding;
    }
}
