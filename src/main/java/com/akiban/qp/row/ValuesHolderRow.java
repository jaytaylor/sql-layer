
package com.akiban.qp.row;


import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.pvalue.PValue;

public class ValuesHolderRow extends AbstractValuesHolderRow {

    // ValuesHolderRow interface -- mostly just promoting visiblity

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public ValueHolder holderAt(int index) {
        return super.holderAt(index);
    }

    @Override
    public PValue pvalueAt(int index) {
        return super.pvalueAt(index);
    }

    @Deprecated
    public ValuesHolderRow(RowType rowType) {
        this(rowType, false);
    }

    public ValuesHolderRow(RowType rowType, boolean usePValues) {
        super(rowType, true, usePValues);
    }
}
