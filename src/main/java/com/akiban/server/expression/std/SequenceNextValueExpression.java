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

import com.akiban.ais.model.TableName;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class SequenceNextValueExpression extends AbstractBinaryExpression {

    @Scalar("NEXTVAL")
    public static final ExpressionComposer COMPOSER = new BinaryComposer() {
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType,
                                     ExpressionType secondType, ExpressionType resultType) {
            return new SequenceNextValueExpression(AkType.LONG, first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException {
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(0, AkType.VARCHAR);
            return ExpressionTypes.LONG;
        }
    };

    public SequenceNextValueExpression(AkType type, Expression first, Expression second) {
        super(type, first, second);
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append("NEXTVAL(").append(left()).append(", ").append(right()).append(")");
    }

    @Override
    public boolean nullIsContaminating() {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    public String name() {
        return "NEXTVAL";
    }

    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation {

        private InnerEvaluation(List<? extends ExpressionEvaluation> children) {
            super(children);
        }

        @Override
        public ValueSource eval() {
            String schema = left().getString();
            String sequence = right().getString();
            logger.debug("Sequence loading : {}.{}", schema, sequence);

            TableName sequenceName = new TableName (schema, sequence);

            long value = queryContext().sequenceNextValue(sequenceName);
            valueHolder().putLong(value);
            return valueHolder();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(SequenceNextValueExpression.class);
}
