/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.operator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.Strings;
import com.google.common.base.Objects;


public abstract class UnionBase extends Operator {

    UnionBase (Operator left, RowType leftType, Operator right, RowType rightType) {
        ArgumentValidation.notNull("left", left);
        ArgumentValidation.notNull("right", right);
        ArgumentValidation.notNull("leftRowType", leftType);
        ArgumentValidation.notNull("rightRowType", rightType);
        ArgumentValidation.isEQ("leftRowType.fields", leftType.nFields(), "rightRowType.fields", rightType.nFields());
        this.outputRowType = rowType(leftType, rightType);
        this.inputs = Arrays.asList(left, right);
        this.inputTypes = Arrays.asList(leftType, rightType);
        ArgumentValidation.isEQ("inputs.size", inputs.size(), "inputTypes.size", inputTypes.size());
    }
    
    private RowType outputRowType;
    private final List<? extends Operator> inputs;
    private final List<? extends RowType> inputTypes;

    @Override
    public RowType rowType() {
        return outputRowType;
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.unmodifiableList(inputs);
    }
    
    public List<RowType> getInputTypes() {
        return Collections.unmodifiableList(inputTypes);
    }
    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        for (Operator oper : inputs) {
            oper.findDerivedTypes(derivedTypes);
        }
    }

    @Override
    public String describePlan() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, end = inputs.size(); i < end; ++i) {
            Operator input = inputs.get(i);
            sb.append(input);
            if (i + 1 < end)
                sb.append(Strings.nl()).append("UNION").append(Strings.nl());
        }
        return sb.toString();
    }

    
    Operator left() { 
        return inputs.get(0); 
    }
    
    Operator right() { 
        return inputs.get(1); 
    }
    
    Operator operator(int i) { 
        return inputs.get(i);
    }

    RowType inputRowType (int i) {
        return inputTypes.get(i);
    }
    // for use in this package (in ctor and unit tests)

    protected static RowType rowType(RowType rowType1, RowType rowType2) {
        if (rowType1 == rowType2)
            return rowType1;
        if (rowType1.nFields() != rowType2.nFields())
            throw notSameShape(rowType1, rowType2);
        return rowTypeNew(rowType1, rowType2);
    }

    private static RowType rowTypeNew(RowType rowType1, RowType rowType2) {
        TInstance[] types = new TInstance[rowType1.nFields()];
        for(int i=0; i<types.length; ++i) {
            TInstance tInst1 = rowType1.typeInstanceAt(i);
            TInstance tInst2 = rowType2.typeInstanceAt(i);
            if (Objects.equal(tInst1, tInst2))
                types[i] = tInst1;
            else if (tInst1 == null)
                types[i] = tInst2;
            else if (tInst2 == null)
                types[i] = tInst1;
            else if (tInst1.equalsExcludingNullable(tInst2)) {
                types[i] = tInst1.nullability() ? tInst1 : tInst2;
            }
            else
                throw notSameShape(rowType1, rowType2);
        }
        return rowType1.schema().newValuesType(types);
    }

    private static IllegalArgumentException notSameShape(RowType rt1, RowType rt2) {
        return new IllegalArgumentException(String.format("RowTypes not of same shape: %s (%s), %s (%s)",
                rt1, tInstanceOf(rt1),
                rt2, tInstanceOf(rt2)
        ));
    }

    static String tInstanceOf (RowType rt) {
        TInstance[] result = new TInstance[rt.nFields()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = rt.typeInstanceAt(i);
        }
        return Arrays.toString(result);
    }

}
