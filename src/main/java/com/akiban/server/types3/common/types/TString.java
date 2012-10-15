/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types3.common.types;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.StringTruncationException;
import com.akiban.server.expression.std.ExpressionTypes;
import com.akiban.server.types3.TBundle;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TInstanceAdjuster;
import com.akiban.server.types3.TInstanceBuilder;
import com.akiban.server.types3.TInstanceNormalizer;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.sql.types.CharacterTypeAttributes;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Formatter;

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
                PUnderlying.STRING);
        this.fixedLength = fixedLength;
        this.typeId = typeId;
    }
    
    private static enum FORMAT implements TClassFormatter {
        STRING {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(source.getString());
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                formatQuoted(source, out, '\'', '\'', false);
            }

            @Override
            public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
                formatQuoted(source, out, '"', '\\', true);
            }

            private boolean needsEscaping(int ch) {
                // Anything other than printing ASCII.
                return (ch >= 0200) || Character.isISOControl(ch);
            }

            private static final String SIMPLY_ESCAPED = "\r\n\t";
            private static final String SIMPLY_ESCAPES = "rnt";

            protected void formatQuoted(PValueSource source, AkibanAppender out,
                                        char quote, char escape, boolean escapeControlChars) {
                String value = source.getString();
                out.append(quote);
                if (!escapeControlChars && (value.indexOf(quote) < 0))
                    out.append(value);
                else {
                    for (int i = 0; i < value.length(); i++) {
                        int ch = value.charAt(i);
                        if (escapeControlChars && needsEscaping(ch)) {
                            int idx = SIMPLY_ESCAPED.indexOf(ch);
                            if (idx < 0) {
                                new Formatter(out.getAppendable()).format("\\u%04x", (int)ch);
                            }
                            else {
                                out.append(escape);
                                out.append(SIMPLY_ESCAPES.charAt(idx));
                            }
                        }
                        else {
                            if ((ch == quote) || (ch == escape))
                                out.append(escape);
                            out.append((char)ch);
                        }
                    }
                }
                out.append(quote);
            }
        }
    }

    public int getFixedLength() {
        return fixedLength;
    }

    @Override
    protected int doCompare(TInstance instanceA, PValueSource sourceA, TInstance instanceB, PValueSource sourceB) {
        CharacterTypeAttributes aAttrs = StringAttribute.characterTypeAttributes(instanceA);
        CharacterTypeAttributes bAttrs = StringAttribute.characterTypeAttributes(instanceB);
        AkCollator collator = ExpressionTypes.mergeAkCollators(aAttrs, bAttrs);
        if (collator == null)
            // TODO in the future, we may want to use some default collator. For now, just use native comparison
            return sourceA.getString().compareTo(sourceB.getString());
        return collator.compare(sourceA, sourceB);
    }

    @Override
    public void attributeToString(int attributeIndex, long value, StringBuilder output) {
        StringAttribute attribute = StringAttribute.values()[attributeIndex];
        switch (attribute) {
        case LENGTH:
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
                output.append(collator.getName());
            }
            break;
        }
    }

    public AkCollator getCollator(TInstance instance) {
        return AkCollatorFactory.getAkCollator((int)instance.attribute(StringAttribute.COLLATION));
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        return new DataTypeDescriptor(
                typeId, instance.nullability(), instance.attribute(StringAttribute.LENGTH), StringAttribute.characterTypeAttributes(instance));
    }

    @Override
    public TInstance instance(int charsetId, int collationId, boolean nullable) {
        return fixedLength < 0
                ? super.instance(charsetId, StringFactory.DEFAULT_CHARSET.ordinal(), collationId, nullable)
                : super.instance(fixedLength, charsetId, collationId, nullable);
    }

    @Override
    public void putSafety(TExecutionContext context, 
                          TInstance sourceInstance,
                          PValueSource sourceValue,
                          TInstance targetInstance,
                          PValueTarget targetValue)
    {
        // check type safety
        assert getClass().isAssignableFrom(sourceInstance.typeClass().getClass())
                    && getClass().isAssignableFrom(targetInstance.typeClass().getClass())
                : "expected instances of TString";
        
        String raw = sourceValue.getString();
        int maxLen = targetInstance.attribute(StringAttribute.LENGTH);
        
        if (raw.length() > maxLen)
        {   
            String truncated = raw.substring(0, maxLen);
            // TODO: check charset and collation, too
            context.warnClient(new StringTruncationException(raw, truncated));
            targetValue.putString(truncated, null);
        }
        else
            targetValue.putString(raw, null);
    }
     
    @Override
    public TInstance instance(boolean nullable)
    {
        return instance(fixedLength >= 0 ? fixedLength : StringFactory.DEFAULT_LENGTH,
                        StringFactory.DEFAULT_CHARSET.ordinal(),
                        StringFactory.DEFAULT_COLLATION_ID,
                        nullable);
    }

    @Override
    public TInstance instance(int length, boolean nullable)
    {
        return instance(length < 0 ? 0 : length, 
                        StringFactory.DEFAULT_CHARSET.ordinal(),
                        StringFactory.DEFAULT_COLLATION_ID,
                        nullable);
    }
    
    @Override
    public TFactory factory()
    {
        return new StringFactory(this);
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        return doPickInstance(left, right, false, suggestedNullability);
    }

    @Override
    protected void validate(TInstance instance) {
        int length = instance.attribute(StringAttribute.LENGTH);
        int charsetId = instance.attribute(StringAttribute.CHARSET);
        int collaitonid = instance.attribute(StringAttribute.COLLATION);
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
            throw new InvalidArgumentTypeException("can't combine strings " + left + " and " + right);
        int aCollation = left.attribute(StringAttribute.COLLATION);
        int bCollation = right.attribute(StringAttribute.COLLATION);
        if (aCollation == bCollation) {
            pickCollation = aCollation;
        }
        else {
            CharacterTypeAttributes aAttrs = StringAttribute.characterTypeAttributes(left);
            CharacterTypeAttributes bAttrs = StringAttribute.characterTypeAttributes(right);
            AkCollator collator = ExpressionTypes.mergeAkCollators(aAttrs, bAttrs);
            pickCollation = (collator == null) ? -1 : collator.getCollationId();
        }
        int leftLen = left.attribute(StringAttribute.LENGTH);
        int rightLen = right.attribute(StringAttribute.LENGTH);
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
