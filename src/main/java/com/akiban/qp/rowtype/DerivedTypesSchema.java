
package com.akiban.qp.rowtype;

import com.akiban.ais.model.HKey;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DerivedTypesSchema {

    public static Set<RowType> descendentTypes(RowType ancestorType, Set<? extends RowType> allTypes)
    {
        Set<RowType> descendentTypes = new HashSet<>();
        for (RowType type : allTypes) {
            if (type != ancestorType && ancestorType.ancestorOf(type)) {
                descendentTypes.add(type);
            }
        }
        return descendentTypes;
    }

    public synchronized AggregatedRowType newAggregateType(RowType parent, int inputsIndex, List<AggregatorFactory> aggregatorFactories, List<? extends TInstance> pAggrTypes)
    {
        return new AggregatedRowType(this, nextTypeId(), parent, inputsIndex, aggregatorFactories, pAggrTypes);
    }

    public synchronized FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        return new FlattenedRowType(this, nextTypeId(), parent, child);
    }

    public synchronized ProjectedRowType newProjectType(List<? extends Expression> columns, List<? extends TPreparedExpression> tExprs)
    {
        return new ProjectedRowType(this, nextTypeId(), columns, tExprs);
    }

    public ProductRowType newProductType(RowType leftType, UserTableRowType branchType, RowType rightType)
    {
        return new ProductRowType(this, nextTypeId(), leftType, branchType, rightType);
    }

    public synchronized ValuesRowType newValuesType(TInstance... fields)
    {
        return new ValuesRowType(this, nextTypeId(), fields);
    }

    public synchronized ValuesRowType newValuesType(AkType... fields)
    {
        return new ValuesRowType(this, nextTypeId(), fields);
    }

    public HKeyRowType newHKeyRowType(HKey hKey)
    {
        return new HKeyRowType(this, hKey);
    }

    synchronized final int nextTypeId()
    {
        return ++typeIdCounter;
    }

    synchronized final void typeIdToLeast(int minValue) {
        typeIdCounter = Math.max(typeIdCounter, minValue);
    }

    private volatile int typeIdCounter = -1;
}
