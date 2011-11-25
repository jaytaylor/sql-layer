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

import com.akiban.server.error.WrongExpressionArityException;
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

        private static boolean compareS(String leftS, String rightS)
        {
            int l = 0, r = 0;
            int d = 0;
            char [] left = leftS.toCharArray();
            char [] right = rightS.toCharArray();

            int lLimit = left.length, rLimit = right.length;
            if (rLimit == 0)
            {
                if (lLimit != 0 ) return false;
                else return true;
            }
            else if (rLimit == 1) //01, 11
            {
                if (right[0] == '%') return true;
                else if ((right[0] == '_' || right[0] == left[0])&& lLimit == 1) return true;
                else return false;
            }

            if (right[right.length-2] == '\\')
            {
                if (right[right.length-1] != left[left.length-1]) return false;
                lLimit = left.length -1;
                rLimit = right.length -2;
            }
            Loop2:
            while (l < lLimit && r < rLimit)
            {
                char lchar = left[l];
                char rchar = right[r];
                boolean esp = false;
                switch(rchar)
                {
                    case '%':
                        char afterP;
                        do
                        {
                            if (r + 1 == rLimit) return true;
                            afterP = right[++r];
                        }
                        while (afterP == '_' || afterP == '%');
                        if (afterP == '\\' && (right[r+1] == '%' || right[r+1] == '_'))
                        {
                            afterP = right[++r];
                            esp = true;
                        }

                        while (l < lLimit)
                        {
                            lchar = left[l++];
                            if (lchar == afterP)
                            {
                                --l;
                                int oldR = r;
                                while (l < lLimit && r < rLimit)
                                {
                                    lchar = left[l];
                                    rchar = right[r];
                                    if ((rchar == '_' || rchar == '%' ) && !esp) continue Loop2;
                                    esp = false;
                                    if (rchar == '\\' && (right[r+1] == '%' || right[r+1] == '_') )
                                        rchar = right[++r];

                                    if (lchar == rchar)
                                    {
                                        ++l;
                                        ++r;
                                    }
                                    else
                                    {
                                        if (l >= lLimit) return false;
                                        r = oldR;
                                        break;
                                    }
                                }
                                if (l == lLimit)
                                {
                                   break Loop2;
                                }
                                else
                                {
                                    r = oldR;
                                }
                            }
                        }

                        return false; // the only way to amke it out of loop1 is for left to end earlier than right not matching enoujgh

                    case '_':
                        ++r;
                        ++l;
                        break;
                    case '\\':
                        if (right[r+1] == '_' || right[r+1] == '%')
                        {
                            ++d;
                            rchar = right[++r];
                        }

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
            {
                if (r == rLimit -1)
                {
                    if ((right[r] == '%' || right[r] == '_') && right[r-2] != '\\') return true;
                    else return false;

                }
                else if(r < rLimit)
                {
                    while (r < rLimit)
                    {
                        if (right[r++] != '%') return false;
                    }
                    return true;
                }
                else
                {
                    if (left[l-1] == right[r-1] || right[r-1] == '_' || right[r-1] == '%') return true;
                    else return false;
                }

            }
            else
            {
                if (l == lLimit -1 && ((right[r-1] == '_' || right[r-1] == '%') && right[r-2] != '\\'))
                {
                    return true;
                }
                else return false;
            }
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