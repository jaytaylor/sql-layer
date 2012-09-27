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

package com.akiban.server.types3.common.funcs;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.expression.std.ExpressionTypes;
import com.akiban.server.expression.std.Matcher;
import com.akiban.server.expression.std.Matchers;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.akiban.sql.types.CharacterTypeAttributes;

public class TLike extends TOverloadBase
{
    /**
     * 
     * @param stringType
     * @return an arrays of all OverLoads available for the LIKE function 
     * with this specifict string type (type: akString vs Mstring, etc)
     */
    public static TOverload[] create(TClass stringType)
    {
        TLike ret[] = new TLike[LikeType.values().length * 2];
        
        int n = 0;
        for (LikeType t : LikeType.values())
        {
            ret[n++] = new TLike(new int[] {0, 1}, stringType, t);
            // optional escape char
            ret[n++] = new TLike(new int[] {0, 1, 2}, stringType, t);
        }
        
        return ret;
    }
    
    static  enum LikeType
    {
        BLIKE, // case sensitive
        ILIKE, // case insensitive
        LIKE   // depends on collation of argument
    }
    
    
    // caching positions
    private static final int MATCHER_INDEX = 0;
    private static final int TYPE_INDEX = 1;
    
    private final int coverage[];
    private final TClass stringType;
    private final LikeType likeType;
    
    TLike (int c[], TClass sType, LikeType lType)
    {
        coverage = c;
        stringType = sType;
        likeType = lType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(stringType, coverage);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) 
    {
        LikeType likeType = this.likeType;
        if (likeType == LikeType.LIKE) 
        {
            CharacterTypeAttributes strAttrs = StringAttribute.characterTypeAttributes(context.inputTypeAt(0));
            CharacterTypeAttributes keyAttrs = StringAttribute.characterTypeAttributes(context.inputTypeAt(1));
            AkCollator collator = ExpressionTypes.mergeAkCollators(strAttrs, keyAttrs);
            if (collator != null) 
            {
                likeType = collator.isCaseSensitive() ? LikeType.BLIKE : LikeType.ILIKE;
            }
        }
        context.set(TYPE_INDEX, likeType);
    }

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, LazyList<? extends TPreptimeValue> inputs)
    {
        TPreptimeValue result = super.evaluateConstant(context, inputs);
        if (result != null) return result; // Whole thing is constant.

        TPreptimeValue patternPrep = inputs.get(1);
        PValueSource patternValue = patternPrep.value();
        if (patternValue == null) return result; // Pattern not constant
        String pattern = patternValue.getString();

        char esca = '\\';
        if (inputs.size() >= 3) {
            TPreptimeValue escapePrep = inputs.get(2);
            PValueSource escapeValue = escapePrep.value();
            if (escapeValue == null) return result; // Escape not constant
            String escapeString = escapeValue.getString();
            if (escapeString.length() != 1) return result;
            esca = escapeString.charAt(0);
        }

        // Pattern (and any optional escape) are constant: can precompile the matcher.
        LikeType likeType = (LikeType)context.get(TYPE_INDEX);
        if (likeType == null)
            likeType = this.likeType;
        
        Matcher matcher = Matchers.getMatcher(pattern, esca, likeType == LikeType.ILIKE);
        context.set(MATCHER_INDEX, matcher);

        return result;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        LikeType likeType = (LikeType)context.preptimeObjectAt(TYPE_INDEX);
        if (likeType == null)
            likeType = this.likeType;
        
        // Get the cached matcher from prepare time.
        Matcher matcher = (Matcher) context.preptimeObjectAt(MATCHER_INDEX);
        if (matcher == null) {
            String right = inputs.get(1).getString();
            char esca = '\\';
            if (inputs.size() == 3) 
            {
                String escapeString = inputs.get(2).getString();
                if (escapeString.length() != 1)
                    throw new InvalidParameterValueException("Invalid escape character: " + escapeString); 
                esca = escapeString.charAt(0);
            }

            // Get the cached matcher from execute time.
            matcher = (Matcher) context.exectimeObjectAt(MATCHER_INDEX);
        
            if (matcher == null || !matcher.sameState(right, esca)) 
            {
                matcher = Matchers.getMatcher(right, esca, likeType == LikeType.ILIKE);
                context.putExectimeObject(MATCHER_INDEX, matcher);
            }
        }

        String left = inputs.get(0).getString();
        try
        {
            output.putBool(matcher.match(left, 1) >= 0);
        }
        catch (InvalidOperationException e)
        {
            // TODO:
            // What's the new way of issuing a warning?
            output.putNull();
        }
    }

    @Override
    public String displayName()
    {
        return likeType.name();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(AkBool.INSTANCE);
    }
    
}
