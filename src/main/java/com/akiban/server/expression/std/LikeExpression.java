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

package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import java.util.List;


public class LikeExpression extends AbstractBinaryExpression
{
    @Scalar("ilike")
    public static final ExpressionComposer ILIKE_COMPOSER = new InnerComposer(LikeMode.CASE_INSENSITIVE);

    @Scalar("blike")
    public static final ExpressionComposer BLIKE_COMPOSER = new InnerComposer(LikeMode.CASE_SENSITIVE);


    public static enum LikeMode
    {
        CASE_INSENSITIVE, CASE_SENSITIVE
    }

    private static final class InnerComposer extends BinaryComposer
    {
        private final LikeMode mode;

        public InnerComposer (LikeMode mode)
        {
            this.mode = mode;
        }

        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
            for (int n = 0; n < argumentTypes.size(); ++n)
                argumentTypes.set(n, AkType.VARCHAR);
        }


        @Override
        protected Expression compose(Expression first, Expression second)
        {
            return new LikeExpression(first, second, mode);
        }

        @Override
        protected ExpressionType composeType(ExpressionType first, ExpressionType second)
        {
            return ExpressionTypes.BOOL;
        }

    }

    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        private final LikeMode mode;

        public InnerEvaluation (List<? extends ExpressionEvaluation> childrenEval, LikeMode mode)
        {
            super(childrenEval);
            this.mode = mode;
        }

        @Override
        public ValueSource eval()
        {
            ValueSource l = left();
            ValueSource r = right();
            if (l.isNull() || r.isNull()) return NullValueSource.only();

            String left = Extractors.getStringExtractor().getObject(l);
            String right = Extractors.getStringExtractor().getObject(r);

            boolean matched = false;
            switch (mode)
            {
                case CASE_INSENSITIVE: matched = compareS(left.toUpperCase(), right.toUpperCase()); break;
                default: matched = compareS(left, right); break;
            }

            return new ValueHolder(AkType.BOOL, matched);
        }

        // TODO: could use another escape character, rather than using only backslash
        // TODO: could be replaced with recursion (as opposed to loops), which is more elegant, but more expensive?
        private static boolean compareS(String left, String right)
        {
            int l = 0, r = 0;
            int lLimit = left.length(), rLimit = right.length();
            if (rLimit == 0) return lLimit == 0;

            Loop2:
            while (l < lLimit && r < rLimit)
            {
                char lchar = left.charAt(l);
                char rchar = right.charAt(r);

                switch(rchar)
                {
                    case '%':
                        char afterP;
                        boolean esc = false;
                        do
                        {
                            if (r + 1 == rLimit) return true;
                            afterP = right.charAt(++r);
                        }
                        while (afterP == '%'); // %%%% is no different than %, so skip multiple %s
                        if (afterP == '\\' && r +1 < rLimit && (right.charAt(r+1) == '%' || right.charAt(r+1) == '_'))
                        {
                            afterP = right.charAt(++r);
                            esc = true;
                        }

                        while (l < lLimit) // loop1: attempt to find a matching sequence in left that starts with afterP
                        {
                            lchar = left.charAt(l++);
                            if (lchar == afterP || afterP == '_' ) // found a *potentially* matching sequence
                            {
                                --l;
                                int oldR = r;
                                while (l < lLimit && r < rLimit)
                                {
                                    lchar = left.charAt(l);
                                    rchar = right.charAt(r);
                                    if ((rchar == '_' || rchar == '%' ) && !esc) // encounter a wildcard char , meaning the sequence indeed matched
                                        continue Loop2; // move on to next search
                                    esc = false;
                                    if (rchar == '\\' && r +1 <rLimit && (right.charAt(r+1) == '%' || right.charAt(r+1) == '_'))
                                        rchar = right.charAt(++r);

                                    if (lchar == rchar)
                                    {
                                        ++l;
                                        ++r;
                                    }
                                    else
                                    {
                                        if (l >= lLimit) return false; // end of left string, lchar didn't match => false
                                        r = oldR;
                                        break;
                                    }
                                }
                                if (l == lLimit) break Loop2; // end of left string (the sequence is matching so far)
                                else r = oldR; // the sequence didn't match, reset counter, search for next sequence
                            }
                        }
                        return false; // the only way to make it out of loop1 is for left to end earlier than right not matching ANY char at all
                    case '_':
                        ++r;
                        ++l;
                        break;
                    case '\\':
                        if ( r + 1 < rLimit && (right.charAt(r+1) == '_' || right.charAt(r+1) == '%')) rchar = right.charAt(++r);//fall thru
                    default:
                        if (lchar != rchar)   return false;
                        else
                        {
                            ++l;
                            ++r;
                        }
                }
            }

            if (l == lLimit)
                if (r < rLimit)
                {
                    while (r < rLimit)
                        if (right.charAt(r++) != '%') return false; // and r-1 != \\
                    return true;
                }
                else return left.charAt(l - 1) == right.charAt(r - 1) || right.charAt(r - 1) == '_' || right.charAt(r - 1) == '%';
            else return false;
        }
    }

    private final LikeMode mode;

    public LikeExpression (Expression left, Expression right, LikeMode mode)
    {
        super(AkType.BOOL, left, right);
        this.mode = mode;
    }


    @Override
    protected boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("LIKE_");
        sb.append(mode);
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(this.childrenEvaluations(), mode);
    }
}