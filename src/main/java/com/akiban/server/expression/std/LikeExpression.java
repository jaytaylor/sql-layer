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
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;
import java.util.List;


public class LikeExpression extends AbstractCompositeExpression
{
    @Scalar("ilike")
    public static final ExpressionComposer ILIKE_COMPOSER = new InnerComposer(true);

    @Scalar("blike")
    public static final ExpressionComposer BLIKE_COMPOSER = new InnerComposer(false);

    @Scalar("like")
    public static final ExpressionComposer LIKE_COMPOSER = ILIKE_COMPOSER;


    private static final class InnerComposer implements ExpressionComposer
    {
        private final boolean case_insensitive;
        public InnerComposer (boolean mode)
        {
            this.case_insensitive = mode;
        }

        @Override
        public String toString ()
        {
            return "LIKE " + (case_insensitive ? "IN" : "" ) + "SENSITIVE";
        }

        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            return new LikeExpression(arguments, case_insensitive);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int s = argumentTypes.size();
            if (s!= 2 && s != 3) throw new WrongExpressionArityException(2, s);
            for (int n = 0; n < s; ++n)
                argumentTypes.setType(n, AkType.VARCHAR);

            return ExpressionTypes.BOOL;
        }
    }

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private final boolean case_insens;
        private char esca = '\\';
        private boolean noWildcardU = false;
        private boolean noWildcardP = false;

        public InnerEvaluation (List<? extends ExpressionEvaluation> childrenEval, boolean mode)
        {
            super(childrenEval);
            this.case_insens = mode;
        }

        @Override
        public ValueSource eval()
        {
            ValueSource l = this.children().get(0).eval();
            ValueSource r = this.children().get(1).eval();
            if (l.isNull() || r.isNull()) return NullValueSource.only();

            String left = Extractors.getStringExtractor().getObject(l);
            String right = Extractors.getStringExtractor().getObject(r);

            if (children().size() == 3)
            {
                ValueSource escapSource = children().get(2).eval();
                if (escapSource.isNull()) return NullValueSource.only();
                esca = escapSource.getString().charAt(0);
            }
            noWildcardU = esca == '_';
            noWildcardP = esca == '%';


            return BoolValueSource.of(compareS(left,right, case_insens));
        }

        private  boolean compareS(String left, String right, boolean case_insensitive)
        {
            int l = 0, r = 0;
            int lLimit = left.length(), rLimit = right.length();
            if (rLimit == 0) return lLimit == 0;

            Loop2:
            while (l < lLimit && r < rLimit)
            {
                char lchar = left.charAt(l);
                char rchar = right.charAt(r);

                if(rchar == '%' && !noWildcardP )
                {
                    char afterP;
                    boolean esc = false;
                    do
                    {
                        if (r + 1 == rLimit) return true;
                        afterP = right.charAt(++r);
                    }
                    while (afterP == '%' );// %%%% is no different than %, so skip multiple %s
                    if (afterP == esca )
                        if (r +1 < rLimit && (right.charAt(r+1) == '%' || right.charAt(r+1) == '_'  || right.charAt(r+1) == esca))
                        {
                            afterP = right.charAt(++r);
                            esc = true;
                        }
                        else throw new UnsupportedOperationException("invalid escape sequence");

                    while (l < lLimit) // loop1: attempt to find a matching sequence in left that starts with afterP
                    {
                        lchar = left.charAt(l++);
                        if (lchar == afterP  || 
                                Character.toUpperCase(lchar) == Character.toUpperCase(afterP) && case_insensitive
                                || afterP == '_' && !noWildcardU && !esc ) // found a *potentially* matching sequence
                        {
                            --l;
                            int oldR = r;
                            boolean oldEsc = esc;
                            while (l < lLimit && r < rLimit)
                            {
                                lchar = left.charAt(l);
                                rchar = right.charAt(r);

                                if (rchar == esca && !esc)
                                {
                                    if (r +1 <rLimit && (right.charAt(r+1) == '%' || right.charAt(r+1) == '_' || right.charAt(r+1) == esca))
                                    {
                                        rchar = right.charAt(++r);
                                        esc = true;
                                    }
                                     else throw new UnsupportedOperationException ("invalid escape sequence");
                                }

                                if (rchar == '%' && !esc) continue Loop2;

                                if (lchar == rchar || 
                                        Character.toUpperCase(lchar) == Character.toUpperCase(rchar) && case_insensitive
                                        || rchar == '_' && !esc && !noWildcardU)
                                {
                                    ++l;
                                    ++r;
                                    esc = false;
                                }
                                else
                                {
                                    if (l >= lLimit) return false; // end of left string, lchar didn't match => false
                                    r = oldR;
                                    break;
                                }
                            }
                            if (l == lLimit) break Loop2; // end of left string (the sequence is matching so far)
                            else
                            {
                                esc = oldEsc;
                                r = oldR;
                            }// the sequence didn't match, reset to initial state, search for next sequence in left
                        }
                    }
                    return false; // the only way to make it out of loop1 is for left to end earlier than right not matching ANY char at all
                }
                else if (rchar == '_' && !noWildcardU )
                {
                    ++r;
                    ++l;
                }
                else if (rchar == esca)
                {
                    if ( r + 1 < rLimit && (right.charAt(r+1) == '_' || right.charAt(r+1) == '%' || right.charAt(r+1) == esca)) rchar = right.charAt(++r);
                    else throw new UnsupportedOperationException("invalid escape sequence"); // TODO: replaced with invalid parameter value

                    if (lchar == rchar || Character.toUpperCase(lchar) == Character.toUpperCase(rchar) && case_insensitive )
                    {
                        ++l;
                        ++r;
                    }
                    else return false;
                }
                else
                {
                    if (lchar == rchar || Character.toUpperCase(lchar) == Character.toUpperCase(rchar) && case_insensitive )
                    {
                        ++l;
                        ++r;
                    }
                    else return false;
                }
            }

            if (l == lLimit)
                if (r < rLimit)
                {
                    while (r < rLimit)
                    {
                        if (right.charAt(r) != '%') return false; // and r-1 != escape
                        else if (r + 1 < rLimit && (right.charAt(r+1) == '%'  || right.charAt(r+1) == '_')&& noWildcardP) return false; // % is  escaped
                        else if (noWildcardP) throw new UnsupportedOperationException("invalid escape sequence"); // % is by itself (invalide escape sequence)
                        ++r;
                    }
                    return true;
                }
                else return true;
            else return false;
        }
    }

    private final boolean case_insens;
    public LikeExpression (List <? extends Expression> children, boolean mode)
    {
        super(AkType.BOOL, checkArgs(children));
        this.case_insens = mode;
    }

    private static List<? extends Expression> checkArgs(List<? extends Expression> children)
    {
        int s = children.size();
        if (s != 2 && s != 3) throw new WrongExpressionArityException(2, s);
        return children;
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
        sb.append(case_insens? "IN" : "");
        sb.append("SENSITIVE");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations(), case_insens);
    }
}
