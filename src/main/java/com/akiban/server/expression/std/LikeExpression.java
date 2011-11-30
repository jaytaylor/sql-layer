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


public class LikeExpression extends AbstractCompositeExpression
{
    @Scalar("ilike")
    public static final ExpressionComposer ILIKE_COMPOSER = new InnerComposer(LikeMode.CASE_INSENSITIVE);

    @Scalar("blike")
    public static final ExpressionComposer BLIKE_COMPOSER = new InnerComposer(LikeMode.CASE_SENSITIVE);

    @Scalar("like")
    public static final ExpressionComposer LIKE_COMPOSER = ILIKE_COMPOSER;

    public static enum LikeMode
    {
        CASE_INSENSITIVE, CASE_SENSITIVE
    }

    private static final class InnerComposer implements ExpressionComposer
    {
        private final LikeMode mode;
        public InnerComposer (LikeMode mode)
        {
            this.mode = mode;
        }

        @Override
        public String toString ()
        {
            return "LIKE " + mode;
        }

        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
            for (int n = 0; n < argumentTypes.size(); ++n)
                argumentTypes.set(n, AkType.VARCHAR);
        }


        @Override
        public ExpressionType composeType(List<? extends ExpressionType> argumentTypes) 
        {
            int s = argumentTypes.size();
            if (s!= 2 && s != 3) throw new WrongExpressionArityException(2, s);
            return ExpressionTypes.BOOL;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            return new LikeExpression(arguments, mode);
        }
    }

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private final LikeMode mode;
        private char esca = '\\';
        private boolean noWildcardU = false;
        private boolean noWildcardP = false;

        public InnerEvaluation (List<? extends ExpressionEvaluation> childrenEval, LikeMode mode)
        {
            super(childrenEval);
            this.mode = mode;
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
            
            boolean matched = false;  
            switch (mode)
            {
                case CASE_INSENSITIVE: matched = compareS(left.toUpperCase(), right.toUpperCase()); break;
                default: matched = compareS(left, right); break;
            }

            return new ValueHolder(AkType.BOOL, matched);
        }

        private  boolean compareS(String left, String right)
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
                        if (lchar == afterP || afterP == '_' && !noWildcardU && !esc ) // found a *potentially* matching sequence
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
                                             
                                if (lchar == rchar || rchar == '_' && !esc && !noWildcardU)
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
                    
                    if (lchar != rchar)   return false;
                    else
                    {
                        ++l;
                        ++r;
                    }
                }
                else
                {
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
                    {
                        if (right.charAt(r) != '%') return false; // and r-1 != escape 
                        else if (r + 1 < rLimit && right.charAt(r+1) == '%' && noWildcardP) return false; // % is  escaped
                        else if (noWildcardP) throw new UnsupportedOperationException("invalid escape sequence"); // % is by itself (invalide escape sequence)
                        ++r;
                    }
                    return true;
                }
                else return left.charAt(l - 1) == right.charAt(r - 1) || right.charAt(r - 1) == '_' || right.charAt(r - 1) == '%';
            else return false;
        }
    }

    private final LikeMode mode;

    public LikeExpression (List <? extends Expression> children, LikeMode mode)
    {
        super(AkType.BOOL, checkArgs(children));
        this.mode = mode;
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
        sb.append(mode);
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations(), mode);
    }
}
