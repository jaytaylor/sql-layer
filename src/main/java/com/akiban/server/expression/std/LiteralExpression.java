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
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.server.types.util.SqlLiteralValueFormatter;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LiteralExpression implements Expression {

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
        return  evaluation;
    }

    @Override
    public AkType valueType() {
        return evaluation.eval().getConversionType();
    }

    public LiteralExpression(ValueSource source) {
        this(new InternalEvaluation(new ValueHolder(source)));
    }

    public LiteralExpression(AkType type, long value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, double value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, float value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, boolean value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, Object value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }
    
    private LiteralExpression(InternalEvaluation evaluation) {
        this.evaluation = evaluation;
      }
    
    public static Expression forNull() {
        return NULL_EXPRESSION;
    }

    public static Expression forBool(Boolean value) {
        if (value == null)
            return BOOL_NULL;
        return value ? BOOL_TRUE : BOOL_FALSE;
    }

    // Object interface

    @Override
    public String toString() {
        return "Literal(" + evaluation.eval().toString() + ')';
    }

    // object state

    private final ExpressionEvaluation evaluation;
    
    // const

    private static final InternalEvaluation NULL_EVAL  = new InternalEvaluation(NullValueSource.only()) ;
    private static final Expression NULL_EXPRESSION = new LiteralExpression(NULL_EVAL);
    private static final Expression BOOL_TRUE = new LiteralExpression(new InternalEvaluation(BoolValueSource.OF_TRUE));
    private static final Expression BOOL_FALSE = new LiteralExpression(new InternalEvaluation(BoolValueSource.OF_FALSE));
    private static final Expression BOOL_NULL = new LiteralExpression(new InternalEvaluation(BoolValueSource.OF_NULL));

    @Override
    public String name()
    {
        return "LITERAL";
    }

    @Override
    public Explainer getExplainer()
    {
        StringBuilder sb = new StringBuilder();
        SqlLiteralValueFormatter formatter = new SqlLiteralValueFormatter(sb);
        try {
            formatter.append(evaluation.eval());
        } catch (IOException ex) {
            Logger.getLogger(LiteralExpression.class.getName()).log(Level.SEVERE, null, ex);
        }
        return PrimitiveExplainer.getInstance(sb.toString());
    }
    
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    // nested classes
    
    private static class InternalEvaluation extends ExpressionEvaluation.Base {
        @Override
        public void of(Row row) {
        }

        @Override
        public void of(QueryContext context) {
        }

        @Override
        public ValueSource eval() {
            return valueSource;
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

        private InternalEvaluation(ValueSource valueSource) {
            this.valueSource = valueSource;
        }

        private final ValueSource valueSource;
    }
}
