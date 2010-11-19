package com.akiban.cserver.encoding;

import com.akiban.ais.model.Type;

public final class EncoderFactory {
    private EncoderFactory() {
    }

    public static final IntEncoder INT = new IntEncoder();
    public static final UIntEncoder U_INT = new UIntEncoder();
    public static final FloatEncoder FLOAT = new FloatEncoder();
    public static final UFloatEncoder U_FLOAT = new UFloatEncoder();
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
    public static final UnsupportedTypeEncoder BIT = new UnsupportedTypeEncoder("BIT");

    private enum Encodings {
        INT (EncoderFactory.INT),
        U_INT (EncoderFactory.U_INT),
        FLOAT (EncoderFactory.FLOAT),
        U_FLOAT (EncoderFactory.U_FLOAT),
        DECIMAL (EncoderFactory.DECIMAL),
        U_DECIMAL (EncoderFactory.U_DECIMAL),
        VARCHAR (EncoderFactory.VARCHAR),
        VARBINARY (EncoderFactory.VARBINARY),
        BLOB (EncoderFactory.BLOB),
        TEXT (EncoderFactory.TEXT),
        DATE (EncoderFactory.DATE),
        TIME (EncoderFactory.TIME),
        DATETIME (EncoderFactory.DATETIME),
        TIMESTAMP (EncoderFactory.TIMESTAMP),
        YEAR (EncoderFactory.YEAR),
        BIT (EncoderFactory.BIT),
        ;

        private final Encoding<?> encoding;

        private Encodings(Encoding<?> encoding) {
            this.encoding = encoding;
        }

        public Encoding<?> getEncoding() {
            return encoding;
        }
    }

    /**
     * Gets an encoding by name.
     * @param name the encoding's name
     * @return the encoding
     * @throws EncodingException if no such encoding exists
     */
    private static Encoding<?> valueOf(String name) {
        try {
            return Encodings.valueOf(name).getEncoding();
        } catch (IllegalArgumentException e) {
            throw new EncodingException("No such encoding: " + name);
        }
    }

    /**
     * Gets an encoding by name, also verifying that it's valid for the given type
     * @param name the encoding's name
     * @param type the type to verify
     * @return the encoding
     * @throws EncodingException if the type is invalid for this encoding, or if the encoding doesn't exist
     */
    public static Encoding<?> valueOf(String name, Type type) {
        Encoding<?> encoding = valueOf(name);

        if (!encoding.validate(type)) {
            throw new EncodingException("Encoding " + encoding + " not valid for type " + type);
        }
        return encoding;
    }
}
