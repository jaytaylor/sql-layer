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

package com.akiban.server.expression.std;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.server.types.util.ValueSources;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;

import java.util.EnumMap;
import java.util.List;

public class CompareExpression extends AbstractBinaryExpression {

    @Scalar("equals") public static final ExpressionComposer EQ_COMPOSER = new InnerComposer(Comparison.EQ);
    @Scalar("greaterOrEquals") public static final ExpressionComposer GE_COMPOSER = new InnerComposer(Comparison.GE);
    @Scalar("greaterThan") public static final ExpressionComposer GT_COMPOSER = new InnerComposer(Comparison.GT);
    @Scalar("lessOrEquals") public static final ExpressionComposer LE_COMPOSER = new InnerComposer(Comparison.LE);
    @Scalar("lessThan") public static final ExpressionComposer LT_COMPOSER = new InnerComposer(Comparison.LT);
    @Scalar("notEquals") public static final ExpressionComposer NE_COMPOSER = new InnerComposer(Comparison.NE);

    // AbstractTwoArgExpression interface
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(name());
        // TODO: Need nicer presentation.
        if (collator != null)
            sb.append("/").append(collator);
    }
    
    @Override
    public String name () {
        return comparison.name();
    }

    @Override
    public Explainer getExplainer () {
        Explainer ex = new ExpressionExplainer(Type.BINARY_OPERATOR, name(), children());
        ex.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance(comparison.toString()));
        return ex;
    }
    
    @Override
    public ExpressionEvaluation evaluation() {
        if (collator != null)
            return new CollateEvaluation(childrenEvaluations(), OPS.get(comparison), collator);
        else
            return new CompareEvaluation(childrenEvaluations(), OPS.get(comparison));
    }

    @Override
    public boolean nullIsContaminating() {
        return true;
    }

    public CompareExpression(Expression lhs, Comparison comparison, Expression rhs, AkCollator collator) {
        this(AkType.BOOL, lhs, comparison, rhs, collator);
    }

    /*
     * Old version
    public CompareExpression(Expression lhs, Comparison comparison, Expression rhs) {
        super(AkType.BOOL, lhs, rhs);
        this.comparison = comparison;
        AkType type = childrenType(children());
        assert type != null;
        this.op = readOnlyCompareOps.get(type);
        if (this.op == null)
            throw new AkibanInternalException("couldn't find internal comparator for " + type);
        //this(AkType.BOOL, lhs, comparison, rhs);
    }
    */
    
    //copied from trunk
    public CompareExpression(Expression lhs, Comparison comparison, Expression rhs) {
        this(AkType.BOOL, lhs, comparison, rhs, null);
    }
    
    // For use by RankExpression
    protected CompareExpression(Expression lhs, Expression rhs) {
        this(AkType.INT, lhs, null, rhs, null);
    }
    
    // for use in this class

    private CompareExpression(AkType outputType, Expression lhs, Comparison comparison, Expression rhs, AkCollator collator)
    {
        super(outputType, lhs, rhs);
        this.comparison = comparison;
        this.collator = collator;
    }
    
    // overriding protected methods

    @Override
    protected void buildToString(StringBuilder sb) {//Field(2) < Literal(8888)
        sb.append(left()).append(' ').append(comparison).append(' ').append(right());
    }


    // object state

    private final Comparison comparison;
    private final AkCollator collator;

     // consts
    
    private static final EnumMap<Comparison, Op> OPS= new EnumMap<Comparison, Op>(Comparison.class);
    
    static
    {
        OPS.put(Comparison.EQ, 
               new Op() {
                   public boolean compare(ValueSource a, ValueSource b) { 
                       return ValueSources.equals(a, b, false);
                   }
                   public boolean collate(AkCollator c, ValueSource a, ValueSource b) { 
                       return c.compare(a, b) == 0;
                   }
               } );
        
        OPS.put(Comparison.GE,
               new Op() {
                   public boolean compare(ValueSource a, ValueSource b) { 
                       return ValueSources.compare(a, b) >= 0; 
                   }
                   public boolean collate(AkCollator c, ValueSource a, ValueSource b) { 
                       return c.compare(a, b) >= 0;
                   }
               } );
        
        OPS.put(Comparison.GT,
               new Op() {
                   public boolean compare(ValueSource a, ValueSource b) { 
                       return ValueSources.compare(a, b) > 0; 
                   }
                   public boolean collate(AkCollator c, ValueSource a, ValueSource b) { 
                       return c.compare(a, b) > 0;
                   }
               } );
        
        OPS.put(Comparison.LE,
               new Op() {
                   public boolean compare(ValueSource a, ValueSource b) { 
                       return ValueSources.compare(a, b) <= 0;
                   }
                   public boolean collate(AkCollator c, ValueSource a, ValueSource b) { 
                       return c.compare(a, b) <= 0;
                   }
               } );
        
        OPS.put(Comparison.LT,
               new Op() {
                   public boolean compare(ValueSource a, ValueSource b) { 
                       return ValueSources.compare(a, b) < 0;
                   }
                   public boolean collate(AkCollator c, ValueSource a, ValueSource b) { 
                       return c.compare(a, b) < 0;
                   }
               } );
        
        OPS.put(Comparison.NE,
               new Op() {
                   public boolean compare(ValueSource a, ValueSource b) { 
                       return !ValueSources.equals(a, b, false);
                   }
                   public boolean collate(AkCollator c, ValueSource a, ValueSource b) { 
                       return c.compare(a, b) != 0;
                   }
               } );
    }

    // nested classes
    
    protected interface Op
    {
        boolean compare(ValueSource a, ValueSource b);
        boolean collate(AkCollator c, ValueSource a, ValueSource b);
    }
       
   

    private static class CompareEvaluation extends AbstractTwoArgExpressionEvaluation {
        @Override
        public ValueSource eval() {
            ValueSource left = left();
            ValueSource right = right();
            if (left.isNull() || right.isNull())
                return BoolValueSource.OF_NULL;

            return BoolValueSource.of(op.compare(left, right));
        }

        private CompareEvaluation(List<? extends ExpressionEvaluation> children, Op op) {
            super(children);
            this.op = op;
        }

        private final Op op;
    }

    private static class CollateEvaluation extends AbstractTwoArgExpressionEvaluation {
        @Override
        public ValueSource eval() {
            ValueSource left = left();
            ValueSource right = right();
            if (left.isNull() || right.isNull())
                return BoolValueSource.OF_NULL;

            return BoolValueSource.of(op.collate(collator, left, right));
        }

        private CollateEvaluation(List<? extends ExpressionEvaluation> children, Op op, AkCollator collator) {
            super(children);
            this.op = op;
            this.collator = collator;
        }

        private final Op op;
        private final AkCollator collator;
    }

    private static final class InnerComposer extends BinaryComposer {

        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType) {
            return new CompareExpression(first, comparison, second, ExpressionTypes.operationCollation(firstType, secondType));
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            return ExpressionTypes.BOOL;
        }

        private InnerComposer(Comparison comparison) {
            this.comparison = comparison;
        }

        private final Comparison comparison;
    }
}
