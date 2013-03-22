
package com.akiban.qp.rowtype;

import com.akiban.server.types.AkType;

import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.types3.TInstance;

import java.util.List;

public final class AggregatedRowType extends DerivedRowType {
    @Override
    public int nFields() {
        return base.nFields();
    }

    @Override
    public AkType typeAt(int index) {
        if (index < inputsIndex)
            return base.typeAt(index);
        else
            return aggregatorFactories.get(index - inputsIndex).outputType();
    }
    
    @Override
    public TInstance typeInstanceAt(int index) {
        if (index < inputsIndex)
            return base.typeInstanceAt(index);
        else
            return pAggrTypes.get(index - inputsIndex);
    }

    public AggregatedRowType(DerivedTypesSchema schema, int typeId,
                             RowType base, int inputsIndex, List<AggregatorFactory> aggregatorFactories) {
        this(schema, typeId, base, inputsIndex, aggregatorFactories, null);
    }

    public AggregatedRowType(DerivedTypesSchema schema, int typeId,
                             RowType base, int inputsIndex, List<AggregatorFactory> aggregatorFactories,
                             List<? extends TInstance> pAggrTypes) {
        super(schema, typeId);
        this.base = base;
        this.inputsIndex = inputsIndex;
        this.aggregatorFactories = aggregatorFactories;
        this.pAggrTypes = pAggrTypes;
    }

    private final RowType base;
    private final int inputsIndex;
    private final List<AggregatorFactory> aggregatorFactories;
    private final List<? extends TInstance> pAggrTypes;
}
