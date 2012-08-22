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
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.explain.Type;
import com.akiban.server.explain.std.ExpressionExplainer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.List;
import java.util.Map;

public final class FieldExpression implements Expression {

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
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(rowType, fieldIndex, valueType());
    }

    @Override
    public AkType valueType() {
        return rowType.typeAt(fieldIndex);
    }

    // Object interface


    @Override
    public String toString() {
        return String.format("Field(%d)", fieldIndex);
    }

    public FieldExpression(RowType rowType, int fieldIndex) {
        this.rowType = rowType;
        this.fieldIndex = fieldIndex;
        if (this.fieldIndex < 0 || this.fieldIndex >= rowType.nFields()) {
            throw new IllegalArgumentException("fieldIndex out of range: " + this.fieldIndex + " for " + this.rowType);
        }
    }

    private final RowType rowType;
    private final int fieldIndex;

    @Override
    public String name()
    {
        return "Field";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new ExpressionExplainer(Type.FUNCTION, name(), context, (List)null);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(fieldIndex));
        ex.addAttribute(Label.ROWTYPE, rowType.getExplainer(context));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    // nested classes

    private static class InnerEvaluation extends ExpressionEvaluation.Base {

        // ExpressionEvaluation interface

        @Override
        public void of(Row row) {
            RowType incomingType = row.rowType();
            if (!rowType.equals(incomingType)) {
                throw new IllegalArgumentException("wrong row type: " + incomingType + " != " + rowType);
            }
            ValueSource incomingSource = row.eval(fieldIndex);
            AkType incomingAkType = incomingSource.getConversionType();
            if (incomingAkType != AkType.NULL && !akType.equals(incomingAkType)) {
                throw new AkibanInternalException(
                        row + "[" + fieldIndex + "] had akType " + incomingAkType + "; expected " + akType
                );
            }
            this.row = row;
        }

        @Override
        public void of(QueryContext context) {
        }

        @Override
        public ValueSource eval() {
            if (row == null)
                throw new IllegalStateException("haven't seen a row to target");
            return row.eval(fieldIndex);
        }

        // Shareable interface

        @Override
        public void acquire() {
            row.acquire();
        }

        @Override
        public boolean isShared() {
            return row.isShared();
        }

        @Override
        public void release() {
            row.release();
        }

        // private methods

        private InnerEvaluation(RowType rowType, int fieldIndex, AkType akType) {
            assert rowType != null;
            assert akType != null;
            this.rowType = rowType;
            this.fieldIndex = fieldIndex;
            this.akType = akType;
        }

        private final RowType rowType;
        private final int fieldIndex;
        private final AkType akType;
        private Row row;
    }
}
