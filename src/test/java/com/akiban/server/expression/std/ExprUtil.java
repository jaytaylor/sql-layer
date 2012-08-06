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

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.optimizer.explain.Explainer;
import java.util.Map;

final class ExprUtil {

    public static Expression lit(String string) {
        return new LiteralExpression(AkType.VARCHAR, string);
    }

    public static Expression lit(double value) {
        return new LiteralExpression(AkType.DOUBLE, value);
    }

    public static Expression lit(long value) {
        return new LiteralExpression(AkType.LONG, value);
    }

    public static Expression lit(boolean value) {
        return new LiteralExpression(AkType.BOOL, value);
    }

    public static Expression constNull() {
        return constNull(AkType.NULL);
    }

    public static Expression constNull(AkType type) {
        return new TypedNullExpression(type);
    }

    public static Expression nonConstNull(AkType type) {
        return nonConst(constNull(type));
    }

    public static Expression exploding(AkType type) {
        return new ExplodingExpression(type);
    }

    public static Expression nonConst(long value) {
        return nonConst(lit(value));
    }

    private static Expression nonConst(Expression expression) {
        return new NonConstWrapper(expression);
    }

    private ExprUtil() {}

    private static final class TypedNullExpression implements Expression {

        // TypedNullExpression interface

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public boolean needsBindings() {
            return false;
        }

        @Override
        public boolean needsRow() {
            return false;
        }

        @Override
        public ExpressionEvaluation evaluation() {
            return LiteralExpression.forNull().evaluation();
        }

        @Override
        public AkType valueType() {
            return type;
        }

        // object interface

        @Override
        public String toString() {
            return "NULL(type=" + type + ')';
        }

        // use in this class

        TypedNullExpression(AkType type) {
            this.type = type;
        }

        private final AkType type;

        @Override
        public String name()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Explainer getExplainer(Map<Object, Explainer> extraInfo)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public boolean nullIsContaminating()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    private static final class ExplodingExpression implements Expression {

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean needsBindings() {
            return false;
        }

        @Override
        public boolean needsRow() {
            return false;
        }

        @Override
        public ExpressionEvaluation evaluation() {
            return KILLER;
        }

        @Override
        public AkType valueType() {
            return type;
        }

        @Override
        public String toString() {
            return "EXPLODING(" + type + ')';
        }

        ExplodingExpression(AkType type) {
            this.type = type;
        }

        private final AkType type;

        private static final ExpressionEvaluation KILLER = new ExpressionEvaluation.Base() {
            @Override
            public void of(Row row) {
            }

            @Override
            public void of(QueryContext context) {
            }

            @Override
            public ValueSource eval() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void acquire() {
            }

            @Override
            public boolean isShared() {
                return false;
            }

            @Override
            public void release() {
            }

            @Override
            public String toString() {
                return "EXPLOSION_EVAL";
            }
        };

        @Override
        public String name()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Explainer getExplainer(Map<Object, Explainer> extraInfo)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public boolean nullIsContaminating()
        {
            return true;
        }
    }

    private static final class NonConstWrapper implements Expression {

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean needsBindings() {
            return delegate.needsBindings();
        }

        @Override
        public boolean needsRow() {
            return delegate.needsRow();
        }

        @Override
        public ExpressionEvaluation evaluation() {
            return delegate.evaluation();
        }

        @Override
        public AkType valueType() {
            return delegate.valueType();
        }

        @Override
        public String toString() {
            return "NonConst(" + delegate.toString() + ')';
        }

        private NonConstWrapper(Expression delegate) {
            this.delegate = delegate;
        }

        private final Expression delegate;

        @Override
        public String name()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Explainer getExplainer(Map<Object, Explainer> extraInfo)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        
        public boolean nullIsContaminating()
        {
            return true;
        }
    }
}
