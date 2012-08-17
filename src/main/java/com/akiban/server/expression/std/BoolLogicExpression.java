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

import com.akiban.qp.exec.Plannable;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.BooleanExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;

import java.util.List;
import java.util.Map;

public final class BoolLogicExpression extends AbstractBinaryExpression {

    // AbstractTwoArgExpression interface
    
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(logic.name());
    }

    @Override
    public String name () {
        return logic.name();
    }
    
    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo) {
        Explainer ex = new ExpressionExplainer(Type.BINARY_OPERATOR, name(), extraInfo, children());
        ex.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance(name()));
        ex.addAttribute(Label.ASSOCIATIVE, PrimitiveExplainer.getInstance(true));
        return ex;
    }
    
    @Override
    public ExpressionEvaluation evaluation() {
        return new InternalEvaluation(logic, childrenEvaluations());
    }

    @Override
    public boolean nullIsContaminating() {
        return false;
    }
    
    // private ctor -- the composers will be exposed as package-private

    private BoolLogicExpression(Expression lhs, BooleanLogic logic, Expression rhs) {
        super(AkType.BOOL, lhs, rhs);
        this.logic = logic;
    }

    // object state

    private final BooleanLogic logic;

    // class state / consts

    private static final BooleanExtractor extractor = Extractors.getBooleanExtractor();
    private static final BooleanLogic andLogic = new BooleanLogic("AND", false) {
        @Override
        public boolean exec(boolean a, boolean b) {
            return a && b;
        }
    };
    private static final BooleanLogic orLogic = new BooleanLogic("OR", true) {
        @Override
        public boolean exec(boolean a, boolean b) {
            return a || b;
        }
    };

    @Scalar("and")
    public static final ExpressionComposer andComposer = new InternalComposer(andLogic);

    @Scalar("or")
    public static final ExpressionComposer orComposer = new InternalComposer(orLogic);

    // nested classes

    private static abstract class BooleanLogic {
        abstract boolean exec(boolean a, boolean b);
        Boolean trump() {
            return trump;
        }
        String name() {
            return name;
        }

        protected BooleanLogic(String name, boolean trump) {
            this.trump = trump;
            this.name = name;
        }

        private final Boolean trump;
        private final String name;
    }

    private static class InternalComposer extends BinaryComposer {
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType) {
            return new BoolLogicExpression(first, logic, second);
        }

        private InternalComposer(BooleanLogic logic) {
            this.logic = logic;
        }

        private final BooleanLogic logic;

    
        @Override
        public NullTreating getNullTreating() {
            return NullTreating.IGNORE;
        }
        
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            argumentTypes.setType(0, AkType.BOOL);
            argumentTypes.setType(1, AkType.BOOL);
            return ExpressionTypes.BOOL;
        }
    }

    private static class InternalEvaluation extends AbstractTwoArgExpressionEvaluation {

        @Override
        public ValueSource eval() {
            Boolean trump = logic.trump();
            Boolean left = extractor.getBoolean(left(), null);
            if (left == trump)
                return BoolValueSource.of(left);

            Boolean right = extractor.getBoolean(right(), null);
            final Boolean result;
            if (left == null || right == null) {
                result = (right == trump) ? trump : null; // lhs can't be trump; we'd have short-circuited already
            }
            else {
                result = logic.exec(left, right);
            }
            return BoolValueSource.of(result);
        }

        private InternalEvaluation(BooleanLogic logic, List<? extends ExpressionEvaluation> children) {
            super(children);
            this.logic = logic;
        }

        private final BooleanLogic logic;
    }
}
