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

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.error.UnsupportedCharsetException;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TBundle;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TInstanceAdjuster;
import com.foundationdb.server.types.TInstanceBuilder;
import com.foundationdb.server.types.TInstanceNormalizer;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.texpressions.TValidatedOverload;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.ByteSource;
import com.foundationdb.util.Strings;
import com.foundationdb.util.WrappingByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.Formatter;

/**
 * Base types for VARCHAR types. Its base type is UnderlyingType.STRING. Its cached object can either be a String
 * (representing a collated string with a lossy collation) or a ByteSource wrapping the string's bytes.
 */
public abstract class TString extends TClass
{
    protected TString (TypeId typeId, TBundle bundle, String name, int serialisationSize)
    {
        this(typeId, bundle, name, serialisationSize, -1);
    }

    protected TString (TypeId typeId, TBundle bundle, String name, int serialisationSize, int fixedLength)
    {
        super(bundle.id(),
                name,
                AkCategory.STRING_CHAR,
                StringAttribute.class,
                FORMAT.STRING,
                1,
                1,
                serialisationSize,
                UnderlyingType.STRING);
        this.fixedLength = fixedLength;
        this.typeId = typeId;
    }
    
    private static enum FORMAT implements TClassFormatter {
        STRING {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                if (source.hasCacheValue()) {
                    Object cached = source.getObject();
                    if (cached instanceof String) {
                        out.append((String)cached);
                        return;
                    } else if (cached instanceof ByteSource && out.canAppendBytes()) {
                        String tInstanceCharset = StringAttribute.charsetName(type);
                        Charset appenderCharset = out.appendBytesAs();
                        if (Strings.equalCharsets(appenderCharset, tInstanceCharset)) {
                            out.appendBytes((ByteSource) cached);
                            return;
                        }
                    }
                }
                out.append(source.getString());
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                formatQuoted(source, out, "E", '\'', true);
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                formatQuoted(source, out, "", '"', false);
            }

            private boolean needsEscaping(int ch) {
                // Anything other than printing ASCII.
                return (ch < 32) || (ch > 126);
            }

            private static final char ESCAPE = '\\';
            private static final String SIMPLY_ESCAPED = "\r\n\t";
            private static final String SIMPLY_ESCAPES = "rnt";

            protected void formatQuoted(ValueSource source,
                                        AkibanAppender out,
                                        String escapePrefix,
                                        char quote,
                                        boolean canDoubleUpQuote) {
                String value = source.getString();
                boolean containsQuote = false;
                boolean needsEscape = false;
                for(int i = 0; !needsEscape && !containsQuote && i < value.length(); ++i) {
                    char ch = value.charAt(i);
                    containsQuote = (ch == quote);
                    needsEscape = needsEscaping(value.charAt(i));
                }
                if(needsEscape) {
                    out.append(escapePrefix);
                }
                out.append(quote);
                if(containsQuote || needsEscape) {
                    Formatter formatter = new Formatter(out.getAppendable());
                    for(int i = 0; i < value.length(); ++i) {
                        char ch = value.charAt(i);
                        if(needsEscaping(ch)) {
                            int idx = SIMPLY_ESCAPED.indexOf(ch);
                            if(idx < 0) {
                                formatter.format("\\u%04x", (int)ch);
                            } else {
                                out.append(ESCAPE);
                                out.append(SIMPLY_ESCAPES.charAt(idx));
                            }
                        } else {
                            if(ch == quote) {
                                out.append(canDoubleUpQuote ? quote : ESCAPE);
                            } else if(ch == ESCAPE) {
                                out.append(ESCAPE);
                            }
                            out.append(ch);
                        }
                    }

                } else {
                    out.append(value);
                }
                out.append(quote);
            }
        }
    }

    public int getFixedLength() {
        return fixedLength;
    }

    @Override
    public int variableSerializationSize(TInstance type, boolean average) {
        if (fixedLength >= 0) {
            return fixedLength; // Compatible with what old Types-based code did.
        }
        int maxWidth = 1;
        if (!average) {
            try {
                Charset charset = Charset.forName(StringAttribute.charsetName(type));
                if ("UTF-8".equals(charset.name()))
                    maxWidth = 4;   // RFC 3629 (limited to U+10FFFF).
                else
                    maxWidth = (int)charset.newEncoder().maxBytesPerChar();
            }
            catch (IllegalArgumentException ex) {
            }
        }
        return maxWidth * type.attribute(StringAttribute.MAX_LENGTH);
    }

    private int maxCharacterWidth(TInstance type) {
        return 1;
    }

    @Override
    public Object formatCachedForNiceRow(ValueSource source) {
        Object obj = source.getObject(); 
        if (obj instanceof ByteSource) {
            return StringCacher.getString((ByteSource)source.getObject(), source.getType());
        } else {
            assert obj instanceof String : "Value source object not ByteSource nor String: " + source;
            return obj;
        }
    }

    public static AkCollator mergeAkCollators(CharacterTypeAttributes type1Atts, CharacterTypeAttributes type2Atts) {
        CharacterTypeAttributes att;
        try {
            att = CharacterTypeAttributes.mergeCollations(type1Atts, type2Atts);
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
        if (att != null) {
            String coll = att.getCollation();
            if (coll != null)
                return AkCollatorFactory.getAkCollator(coll);
        }
        return null;
    }

    @Override
    protected int doCompare(TInstance typeA, ValueSource sourceA, TInstance typeB, ValueSource sourceB) {
        CharacterTypeAttributes aAttrs = StringAttribute.characterTypeAttributes(typeA);
        CharacterTypeAttributes bAttrs = StringAttribute.characterTypeAttributes(typeB);
        AkCollator collator = mergeAkCollators(aAttrs, bAttrs);
        if (collator == null)
            // TODO in the future, we may want to use some default collator. For now, just use native comparison
            return sourceA.getString().compareTo(sourceB.getString());
        return collator.compare(sourceA, sourceB);
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        return attributeIndex != StringAttribute.MAX_LENGTH.ordinal();
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        return ((attributeIndex == StringAttribute.MAX_LENGTH.ordinal()) &&
                (fixedLength < 0));
    }

    @Override
    public void attributeToString(int attributeIndex, long value, StringBuilder output) {
        StringAttribute attribute = StringAttribute.values()[attributeIndex];
        switch (attribute) {
        case MAX_LENGTH:
            output.append(value);
            break;
        case CHARSET:
            StringFactory.Charset[] charsets = StringFactory.Charset.values();
            if (value < 0 || value >= charsets.length) {
                logger.warn("charset value out of range: {}", value);
                output.append(value);
            }
            else {
                output.append(charsets[(int)value]);
            }
            break;
        case COLLATION:
            AkCollator collator = AkCollatorFactory.getAkCollator((int)value);
            if (collator == null) {
                if (value == StringFactory.NULL_COLLATION_ID) {
                    output.append("NONE");
                }
                else {
                    logger.warn("unknown collator for id " + value + " (" + ((int)value) + ')');
                    output.append(value);
                }
            }
            else {
                output.append(collator.getScheme());
            }
            break;
        }
    }

    @Override
    public Object attributeToObject(int attributeIndex, int value) {
        StringAttribute attribute = StringAttribute.values()[attributeIndex];
        switch (attribute) {
        case MAX_LENGTH:
            return value;
        case CHARSET:
            StringFactory.Charset[] charsets = StringFactory.Charset.values();
            if (value < 0 || value >= charsets.length) {
                logger.warn("charset value out of range: {}", value);
                return value;
            }
            else {
                return charsets[value].name();
            }
        case COLLATION:
            AkCollator collator = AkCollatorFactory.getAkCollator((int)value);
            if (collator == null) {
                if (value == StringFactory.NULL_COLLATION_ID) {
                    return "NONE";
                }
                else {
                    logger.warn("unknown collator for id " + value + " (" + ((int)value) + ')');
                    return value;
                }
            }
            else {
                return collator.getScheme();
            }
        default:
            throw new IllegalArgumentException("illegal attribute index: " + attributeIndex);
        }
    }

    public static AkCollator getCollator(TInstance type) {
        return AkCollatorFactory.getAkCollator((int)type.attribute(StringAttribute.COLLATION));
    }

    @Override
    public ValueCacher cacher() {
        return cacher;
    }

    @Override
    public int jdbcType() {
        if (fixedLength < 0)
            return typeId.getJDBCTypeId(); // [VAR]CHAR
        else
            return Types.LONGVARCHAR; // Not CLOB.
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance type) {
        return new DataTypeDescriptor(
                typeId, type.nullability(), type.attribute(StringAttribute.MAX_LENGTH),
                StringAttribute.characterTypeAttributes(type));
    }

    @Override
    public TInstance instance(int length, int charsetId, int collationId, boolean nullable) {
        return super.instance(fixedLength >= 0 ? fixedLength :
                              length < 0 ? StringFactory.DEFAULT_LENGTH : length,
                              charsetId, collationId, nullable);
    }

    @Override
    public TInstance instance(int length, int charsetId, boolean nullable) {
        return instance(length,
                        charsetId,
                        StringFactory.DEFAULT_COLLATION_ID,
                        nullable);
    }

    @Override
    public TInstance instance(int length, boolean nullable)
    {
        return instance(length,
                        StringFactory.DEFAULT_CHARSET.ordinal(),
                        StringFactory.DEFAULT_COLLATION_ID,
                        nullable);
    }

    @Override
    public TInstance instance(boolean nullable)
    {
        return super.instance(StringFactory.DEFAULT_LENGTH,
                              StringFactory.DEFAULT_CHARSET.ordinal(),
                              StringFactory.DEFAULT_COLLATION_ID,
                              nullable);
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        return doPickInstance(left, right, false, suggestedNullability);
    }

    @Override
    protected void validate(TInstance type) {
        int length = type.attribute(StringAttribute.MAX_LENGTH);
        int charsetId = type.attribute(StringAttribute.CHARSET);
        int collaitonid = type.attribute(StringAttribute.COLLATION);
        // TODO
    }

    @Override
    public TCast castToVarchar() {
        return null;
    }

    @Override
    public TCast castFromVarchar() {
        return null;
    }

    private TInstance doPickInstance(TInstance left, TInstance right, boolean useRightLength, boolean nullable) {
        final int pickLen, pickCharset, pickCollation;

        int aCharset = left.attribute(StringAttribute.CHARSET);
        int bCharset = right.attribute(StringAttribute.CHARSET);
        if (aCharset == bCharset)
            pickCharset = aCharset;
        else
            pickCharset = StringFactory.DEFAULT_CHARSET_ID;
        int aCollation = left.attribute(StringAttribute.COLLATION);
        int bCollation = right.attribute(StringAttribute.COLLATION);
        if (aCollation == bCollation) {
            pickCollation = aCollation;
        }
        else {
            CharacterTypeAttributes aAttrs = StringAttribute.characterTypeAttributes(left);
            CharacterTypeAttributes bAttrs = StringAttribute.characterTypeAttributes(right);
            AkCollator collator = mergeAkCollators(aAttrs, bAttrs);
            pickCollation = (collator == null) ? StringFactory.NULL_COLLATION_ID : collator.getCollationId();
        }
        int leftLen = left.attribute(StringAttribute.MAX_LENGTH);
        int rightLen = right.attribute(StringAttribute.MAX_LENGTH);
        if (useRightLength) {
            pickLen = rightLen;
        }
        else {
            pickLen = Math.max(leftLen,rightLen);
        }
        return instance(pickLen, pickCharset, pickCollation, nullable);
    }

    private final int fixedLength;
    private final TypeId typeId;
    private static final Logger logger = LoggerFactory.getLogger(TString.class);

    private static final ValueCacher cacher = new StringCacher();

    private static class StringCacher implements ValueCacher {
        @Override
        public void cacheToValue(Object cached, TInstance type, BasicValueTarget target) {
            String asString;
            if(cached instanceof String) {
                asString = (String)cached;
            } else if(cached instanceof WrappingByteSource) {
                asString = getString((ByteSource) cached, type);
            } else {
                throw new IllegalStateException("Unexpected cache type: " + cached.getClass());
            }
            target.putString(asString, getCollator(type));
        }

        @Override
        public Object valueToCache(BasicValueSource value, TInstance type) {
            return value.getString();
        }

        @Override
        public Object sanitize(Object object) {
            return String.valueOf(object);
        }

        static String getString(ByteSource bs, TInstance type) {
            String charsetName = StringAttribute.charsetName(type);
            String asString;
            try {
                asString = new String(bs.byteArray(), bs.byteArrayOffset(), bs.byteArrayLength(), charsetName);
            } catch (UnsupportedEncodingException e) {
                throw new UnsupportedCharsetException(charsetName);
            }
            return asString;
        }

        @Override
        public boolean canConvertToValue(Object cached) {
            return cached instanceof ByteSource;
        }
    }

    public final TInstanceNormalizer PICK_RIGHT_LENGTH = new TInstanceNormalizer() {
        @Override
        public void apply(TInstanceAdjuster adapter, TValidatedOverload overload, TInputSet inputSet, int max) {
            TInstance result = null;
            boolean nullable = false;
            for (int i = overload.firstInput(inputSet); i >= 0; i = overload.nextInput(inputSet, i+1, max)) {
                TInstance input = adapter.get(i);
                nullable |= input.nullability();
                result = (result == null)
                        ? input
                        : doPickInstance(result, input, true, nullable);
            }
            assert result != null;
            int resultCharset = result.attribute(StringAttribute.CHARSET);
            int resultCollation = result.attribute(StringAttribute.COLLATION);
            for (int i = overload.firstInput(inputSet); i >= 0; i = overload.nextInput(inputSet, i+1, max)) {
                TInstance input = adapter.get(i);
                int inputCharset = input.attribute(StringAttribute.CHARSET);
                int inputCollation = input.attribute(StringAttribute.COLLATION);
                if ( (inputCharset != resultCharset) || (inputCollation != resultCollation)) {
                    TInstanceBuilder adjusted = adapter.adjust(i);
                    adjusted.setAttribute(StringAttribute.CHARSET, resultCharset);
                    adjusted.setAttribute(StringAttribute.COLLATION, resultCollation);
                }
            }
        }
    };
}
