/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.types3.common.funcs;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.expression.std.ExpressionTypes;
import com.foundationdb.server.expression.std.Matcher;
import com.foundationdb.server.expression.std.Matchers;
import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.TPreptimeContext;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.aksql.aktypes.AkBool;
import com.foundationdb.server.types3.common.types.StringAttribute;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;
import com.foundationdb.sql.types.CharacterTypeAttributes;

public class TLike extends TScalarBase
{
    /**
     * 
     * @param stringType
     * @return an arrays of all OverLoads available for the LIKE function 
     * with this specifict string type (type: akString vs Mstring, etc)
     */
    public static TScalar[] create(TClass stringType)
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
            context.warnClient(e);
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
