
package com.akiban.server.encoding;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.model.Type;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

@SuppressWarnings("unused")
public final class EncoderFactory {
    private EncoderFactory() {
    }

    public static final Encoding INT = LongEncoder.INSTANCE;
    public static final Encoding U_INT = LongEncoder.INSTANCE;
    public static final Encoding U_BIGINT = UBigIntEncoder.INSTANCE;
    public static final Encoding FLOAT = FloatEncoder.INSTANCE;
    public static final Encoding U_FLOAT = FloatEncoder.INSTANCE;
    public static final Encoding DOUBLE = DoubleEncoder.INSTANCE;
    public static final Encoding U_DOUBLE = DoubleEncoder.INSTANCE;
    public static final Encoding DECIMAL = DecimalEncoder.INSTANCE;
    public static final Encoding U_DECIMAL = DecimalEncoder.INSTANCE;
    public static final Encoding VARCHAR = SBCSEncoder.INSTANCE;
    public static final Encoding VARBINARY = VarBinaryEncoder.INSTANCE;
    public static final Encoding BLOB = SBCSEncoder.INSTANCE;  // TODO - temporarily we handle just like TEXT
    public static final Encoding TEXT = SBCSEncoder.INSTANCE;
    public static final Encoding DATE = LongEncoder.INSTANCE;
    public static final Encoding TIME = LongEncoder.INSTANCE;
    public static final Encoding DATETIME = LongEncoder.INSTANCE;
    public static final Encoding TIMESTAMP = LongEncoder.INSTANCE;
    public static final Encoding YEAR = LongEncoder.INSTANCE;
    public static final Encoding BOOLEAN = LongEncoder.INSTANCE;

    private static final Object ENCODING_MAP_LOCK = EncoderFactory.class;
    private static Map<String,Encoding> encodingMap = null;
    private static Map<String,Encoding> charEncodingMap = null;

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
        final Map<String,Encoding> tmp = new HashMap<>();
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
        return valueOf(name);
    }

    public static Encoding valueOf(String name, Type type, String charset) {
        Encoding encoding = valueOf(name);
        if ((encoding == VARCHAR) && (charset != null))
            return charEncoding(charset);
        else
            return encoding;
    }

    /**
     * Get the encoding for a character column.
     */
    public static Encoding charEncoding(String charsetName) {
        synchronized (ENCODING_MAP_LOCK) {
            if (charEncodingMap == null)
                charEncodingMap = new HashMap<>();
            Encoding encoding = encodingMap.get(charsetName);
            if (encoding == null) {
                try {
                    Charset charset = Charset.forName(charsetName);
                    if (charset.name().equals("UTF-8"))
                        encoding = UTF8Encoder.INSTANCE;
                    else if (charset.newEncoder().maxBytesPerChar() == 1.0)
                        encoding = SBCSEncoder.INSTANCE;
                }
                catch (IllegalCharsetNameException ex) {
                    encoding = SBCSEncoder.INSTANCE;
                }
                catch (UnsupportedCharsetException ex) {
                    encoding = SBCSEncoder.INSTANCE;
                }
                catch (UnsupportedOperationException ex) {
                    encoding = SBCSEncoder.INSTANCE;
                }
                if (encoding == null)
                    encoding = new SlowMBCSEncoder(charsetName);
                charEncodingMap.put(charsetName, encoding);
            }
            return encoding;
        }
    }
}
