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

package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTargets;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class AbstractValuesHolderRow extends AbstractRow {

    public boolean usingPValues() {
        return pValues != null;
    }

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueSource eval(int i) {
        assert !usingPValues() : "using pvalues";
        ValueHolder value = values.get(i);
        if (!value.hasSourceState()) {
            throw new IllegalStateException("value at index " + i + " was never set");
        }
        return value;
    }

    @Override
    public PValueSource pvalue(int i) {
        assert usingPValues() : "using old values";
        PValue value = pValues.get(i);
        if (!value.hasAnyValue())
            throw new IllegalStateException("value at index " + i + " was never set");
        return value;
    }

    @Override
    public void acquire() {
        if (isMutable)
            super.acquire();
    }

    @Override
    public void release() {
        if (isMutable)
            super.release();
    }

    @Override
    public boolean isShared() {
        return isMutable && super.isShared();
    }

    // for use by subclasses

    AbstractValuesHolderRow(RowType rowType, boolean isMutable, boolean usePValues) {
        this.isMutable = isMutable;
        this.rowType = rowType;
        int nfields = rowType.nFields();
        if (!usePValues) {
            values = new ArrayList<ValueHolder>();
            for (int i=0; i < nfields; ++i) {
                values.add(new ValueHolder());
            }
            pValues = null;
        }
        else {
            values = null;
            pValues = new ArrayList<PValue>(nfields);
            for (int i = 0; i < nfields; ++i) {
                TInstance tinst = rowType.typeInstanceAt(i);
                TClass underlying = (tinst == null) ? null : tinst.typeClass();
                pValues.add(new PValue(underlying));
            }
        }
    }

    /**
     * @deprecated implies usePValues == false, which is going away
     */
    @Deprecated
    AbstractValuesHolderRow(RowType rowType, boolean isMutable,
                            Iterator<? extends ValueSource> initialValues,
                            Iterator<? extends PValueSource> initialPValues)
    {
        this(rowType, isMutable, initialPValues != null);
        int i = 0;
        if (initialValues != null) {
            assert initialPValues == null : "can't have both old and new expressions";
            while(initialValues.hasNext()) {
                if (i >= values.size())
                    throw new IllegalArgumentException("too many initial values: reached limit of " + values.size());
                ValueSource nextValue = initialValues.next();
                AkType nextValueType = nextValue.getConversionType();
                if (nextValueType != AkType.NULL && nextValueType != rowType.typeAt(i))
                    throw new IllegalArgumentException(
                            "value at index " + i + " expected type " + rowType.typeAt(i)
                                    + ", was " + nextValueType + ": " + nextValue);
                values.get(i++).copyFrom(nextValue);
            }
            if (i != values.size())
                throw new IllegalArgumentException("not enough initial values: required " + values.size() + " but saw " + i);
        }
        else if (initialPValues != null) {
            while(initialPValues.hasNext()) {
                if (i >= pValues.size())
                    throw new IllegalArgumentException("too many initial values: reached limit of " + values.size());
                PValueSource nextValue = initialPValues.next();
                PUnderlying nextValueType = nextValue.getUnderlyingType();
                TInstance expectedTInst = rowType.typeInstanceAt(i);
                PUnderlying expectedValueType = expectedTInst == null ? null : 
                    expectedTInst.typeClass().underlyingType();
                if (nextValueType != expectedValueType)
                    throw new IllegalArgumentException(
                            "value at index " + i + " expected type " + rowType.typeInstanceAt(i)
                                    + ", but PUnderlying was " + nextValueType + ": " + nextValue);
                PValueTargets.copyFrom(nextValue, pValues.get(i++));
            }
            if (i != pValues.size())
                throw new IllegalArgumentException("not enough initial values: required " + values.size() + " but saw " + i);
        }
        else {
            throw new IllegalArgumentException("both expression inputs were null");
        }
    }

    void clear() {
        checkMutable();
        for (ValueHolder value : values) {
            value.clear();
        }
    }

    ValueHolder holderAt(int index) {
        checkMutable();
        return values.get(index);
    }

    PValue pvalueAt(int index) {
        checkMutable();
        return pValues.get(index);
    }

    private void checkMutable() {
        if (!isMutable)
            throw new IllegalStateException("can't invoke method on an immutable AbstractValuesHolderRow");
    }

    private final RowType rowType;
    private final List<ValueHolder> values;
    private final List<PValue> pValues;
    private final boolean isMutable;
}
