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
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.StringTruncationException;
import com.akiban.server.expression.std.ExpressionTypes;
import com.akiban.server.types3.TBundle;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.sql.types.CharacterTypeAttributes;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.akiban.util.AkibanAppender;

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
                String value = source.getString();
                out.append('\'');
                if (value.indexOf('\'') < 0)
                    out.append(value);
                else {
                    for (int i = 0; i < value.length(); i++) {
                        int ch = value.charAt(i);
                        if (ch == '\'')
                            out.append('\'');
                        out.append((char)ch);
                    }
                }
                out.append('\'');
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
        return collator.compare(sourceA.getString(), sourceB.getString());
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
                logger.warn("unknown collator for id " + value + " (" + ((int)value) + ')');
                output.append(value);
            }
            else {
                output.append(collator.getName());
            }
            break;
        }
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        return new DataTypeDescriptor(
                typeId, instance.nullability(), instance.attribute(StringAttribute.LENGTH));
    }

    @Override
    public TInstance instance(int charsetId, int collationId) {
        return fixedLength < 0
                ? super.instance(charsetId, StringFactory.DEFAULT_CHARSET.ordinal(), collationId)
                : super.instance(fixedLength, charsetId, collationId);
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
    public TInstance instance()
    {
        return instance(fixedLength >= 0 ? fixedLength : StringFactory.DEFAULT_LENGTH,
                        StringFactory.DEFAULT_CHARSET.ordinal(),
                        StringFactory.DEFAULT_COLLATION_ID);
    }

    @Override
    public TInstance instance(int length)
    {
        return instance(length < 0 ? 0 : length, 
                        StringFactory.DEFAULT_CHARSET.ordinal(),
                        StringFactory.DEFAULT_COLLATION_ID);
    }
    
    @Override
    public TFactory factory()
    {
        return new StringFactory(this);
    }

    @Override
    protected TInstance doPickInstance(TInstance instanceA, TInstance instanceB)
    {
        final int pickLen, pickCharset, pickCollation;

        int aCharset = instanceA.attribute(StringAttribute.CHARSET);
        int bCharset = instanceB.attribute(StringAttribute.CHARSET);
        if (aCharset == bCharset)
            pickCharset = aCharset;
        else
            throw new InvalidArgumentTypeException("can't combine strings " + instanceA + " and " + instanceB);
        int aCollation = instanceA.attribute(StringAttribute.COLLATION);
        int bCollation = instanceB.attribute(StringAttribute.COLLATION);
        if (aCollation == bCollation) {
            pickCollation = aCollation;
        }
        else {
            CharacterTypeAttributes aAttrs = StringAttribute.characterTypeAttributes(instanceA);
            CharacterTypeAttributes bAttrs = StringAttribute.characterTypeAttributes(instanceB);
            AkCollator collator = ExpressionTypes.mergeAkCollators(aAttrs, bAttrs);
            pickCollation = (collator == null) ? -1 : collator.getCollationId();
        }
        pickLen = Math.max(
                instanceA.attribute(StringAttribute.LENGTH),
                instanceB.attribute(StringAttribute.LENGTH)
        );
        return instance(pickLen, pickCharset, pickCollation);
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

    private final int fixedLength;
    private final TypeId typeId;
    private static final Logger logger = LoggerFactory.getLogger(TString.class);
}
