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

package com.akiban.server.expression.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Type;
import com.akiban.server.explain.std.ExpressionExplainer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.sql.StandardException;
import java.util.List;
import java.util.Map;


public class LikeExpression extends AbstractCompositeExpression
{
    @Scalar("ilike")
    public static final ExpressionComposer ILIKE_COMPOSER = new InnerComposer(Boolean.TRUE);

    @Scalar("blike")
    public static final ExpressionComposer BLIKE_COMPOSER = new InnerComposer(Boolean.FALSE);

    @Scalar("like")
    public static final ExpressionComposer LIKE_COMPOSER = new InnerComposer(null);


    private static final class InnerComposer implements ExpressionComposer
    {
        private final Boolean case_insensitive;
        public InnerComposer (Boolean mode)
        {
            this.case_insensitive = mode;
        }

        @Override
        public String toString ()
        {
            if (case_insensitive == null)
                return "LIKE";
            else
                return "LIKE " + (case_insensitive ? "IN" : "" ) + "SENSITIVE";
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

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            Boolean case_insensitive = this.case_insensitive;
            if (case_insensitive == null) {
                // Figure out case sensitivity from collation.
                if (typesList.size() >= 3) {
                    AkCollator collator = ExpressionTypes.operationCollation(typesList.get(0), typesList.get(1));
                    if (collator != null) {
                        case_insensitive = !collator.isCaseSensitive();
                    }
                }
                if (case_insensitive == null) {
                    case_insensitive = false;
                }
            }
            return new LikeExpression(arguments, case_insensitive);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
    }

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private Matcher matcher = null;
        private final boolean ignore_case;
        
        private boolean isConst;
        private String oldPat;
        private char oldEsc;
        
        public InnerEvaluation (List<? extends ExpressionEvaluation> childrenEval, 
                boolean isConst,
                boolean mode)
        {
            super(childrenEval);
            this.ignore_case = mode;
            this.isConst = isConst;
        }

        @Override
        public ValueSource eval()
        {
            ValueSource l = this.children().get(0).eval();
            ValueSource r = this.children().get(1).eval();
            if (l.isNull() || r.isNull()) return NullValueSource.only();

            
            String left = l.getString();
            String right = r.getString();
            
            char esca;
            if (children().size() == 3)
            {
                ValueSource escapSource = children().get(2).eval();
                if (escapSource.isNull()) return NullValueSource.only();
                String e = escapSource.getString();
                if (e.length() != 1)
                    throw new InvalidParameterValueException("Invalid escape character: " + e);
                esca = escapSource.getString().charAt(0);
            }
            else
                esca = '\\';
            
            if (matcher == null
                    || !isConst && (!right.equals(oldPat) || esca != oldEsc))
            {
                oldPat = right;
                oldEsc = esca;
                matcher = Matchers.getMatcher(right, esca, ignore_case);
            }
                 
           
            try
            {
                return BoolValueSource.of(matcher.match(left, 1) >= 0);
            }
            catch (InvalidOperationException e)
            {
                QueryContext context = queryContext();
                if (context != null)
                    context.warnClient(e);
                return NullValueSource.only();
            }
        }
    }
        
    private final boolean ignore_case;
    public LikeExpression (List <? extends Expression> children, boolean mode)
    {
        super(AkType.BOOL, checkArgs(children));
        this.ignore_case = mode;
    }

    private static List<? extends Expression> checkArgs(List<? extends Expression> children)
    {
        int s = children.size();
        if (s != 2 && s != 3) throw new WrongExpressionArityException(2, s);
        return children;
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    public String name ()
    {
        return "LIKE_" + (ignore_case? "IN" : "") + "SENSITIVE";
    }
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new ExpressionExplainer(Type.BINARY_OPERATOR, name(), context, children());
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
       return new InnerEvaluation(childrenEvaluations()
               , children().size() == 2 
                    ? children().get(1).isConstant()
                    : children().get(1).isConstant() && children().get(2).isConstant()
               , ignore_case);
    }
}
