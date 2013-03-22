
package com.akiban.qp.row;

import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.AkibanAppender;

public class ProductRow extends CompoundRow
{
    // Object interface

    @Override
    public String toString()
    {
        ValueTarget buffer = AkibanAppender.of(new StringBuilder()).asValueTarget();
        ProductRowType type = (ProductRowType)rowType();
        buffer.putString("(");
        int nFields = type.leftType().nFields() + type.rightType().nFields() - type.branchType().nFields();
        for (int i = 0; i < nFields; i++) {
            if (i > 0) {
                buffer.putString(", ");
            }
            Converters.convert(eval(i), buffer);
        }
        buffer.putString(")");
        return buffer.toString();
    }

    // ProductRow interface

    public ProductRow(ProductRowType rowType, Row left, Row right)
    {
        super (rowType, left, right);
        this.rowOffset = firstRowFields() - rowType.branchType().nFields();
        if (left != null && right != null) {
            // assert left.runId() == right.runId();
        }
    }
}
