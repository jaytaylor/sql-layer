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

package com.akiban.qp.operator;

import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.error.*;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.BloomFilter;
import com.akiban.util.SparseArray;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public abstract class QueryContextBase implements QueryContext
{
    private SparseArray<Object> bindings = new SparseArray<Object>();
    // startTimeMsec is used to control query timeouts.
    private final long startTimeMsec = System.currentTimeMillis();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + bindings.describeElements() + "]";
    }

    /* QueryContext interface */

    @Override
    public PValueSource getPValue(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (PValueSource)bindings.get(index);
    }

    @Override
    public void setPValue(int index, PValueSource value) {
        PValue holder = null;
        if (bindings.isDefined(index)) {
            holder = (PValue)bindings.get(index);
            if (holder.getUnderlyingType() != value.getUnderlyingType())
                holder = null;
        }
        if (holder == null) {
            holder = new PValue(value.getUnderlyingType());
            bindings.set(index, holder);
        }
        PValueTargets.copyFrom(value, holder);
    }

    @Override
    public ValueSource getValue(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (ValueSource)bindings.get(index);
    }

    @Override
    public void setValue(int index, ValueSource value, AkType type)
    {
        ValueHolder holder;
        if (bindings.isDefined(index))
            holder = (ValueHolder)bindings.get(index);
        else {
            holder = new ValueHolder();
            bindings.set(index, holder);
        }
        
        holder.expectType(type);
        try
        {
            Converters.convert(value, holder);
        }
        catch (InvalidDateFormatException e)
        {
            errorCase(e, holder);
        }
        catch (InconvertibleTypesException e)
        {
            errorCase(e, holder);
        }
        catch (InvalidCharToNumException e)
        {
            errorCase(e, holder);
        }
    }
    
    private void errorCase (InvalidOperationException e, ValueHolder holder)
    {
        warnClient(e);
        switch(holder.getConversionType())
        {
            case DECIMAL:   holder.putDecimal(BigDecimal.ZERO); break;
            case U_BIGINT:  holder.putUBigInt(BigInteger.ZERO); break;
            case LONG:
            case U_INT:
            case INT:        holder.putRaw(holder.getConversionType(), 0L); break;
            case U_DOUBLE:   
            case DOUBLE:     holder.putRaw(holder.getConversionType(), 0.0d);
            case U_FLOAT:
            case FLOAT:      holder.putRaw(holder.getConversionType(), 0.0f); break;
            case TIME:       holder.putTime(0L);
            default:         holder.putNull();

        }
    }

    @Override
    public void setValue(int index, ValueSource value)
    {
        setValue(index, value, value.getConversionType());
    }

    @Override
    public Row getRow(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (Row)bindings.get(index);
    }

    @Override
    public void setRow(int index, Row row)
    {
        // TODO: Should this use a RowHolder or will that make things worse?
        bindings.set(index, row);
    }

    @Override
    public HKey getHKey(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (HKey)bindings.get(index);
    }

    @Override
    public void setHKey(int index, HKey hKey)
    {
        bindings.set(index, hKey);
    }

    @Override
    public BloomFilter getBloomFilter(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return (BloomFilter)bindings.get(index);
    }

    @Override
    public void setBloomFilter(int index, BloomFilter filter) {
        bindings.set(index, filter);
    }

    @Override
    public void clear() {
        bindings.clear();
    }

    @Override
    public Date getCurrentDate() {
        return new Date();
    }

    @Override
    public String getSystemUser() {
        return System.getProperty("user.name");
    }

    @Override
    public long getStartTime() {
        return startTimeMsec;
    }

    @Override
    public void warnClient(InvalidOperationException exception) {
        notifyClient(NotificationLevel.WARNING, exception.getCode(), exception.getShortMessage());
    }

    @Override
    public long getQueryTimeoutSec() {
        return getStore().getQueryTimeoutSec();
    }

    @Override
    public void checkQueryCancelation() {
        if (getSession().isCurrentQueryCanceled()) {
            throw new QueryCanceledException(getSession());
        }
        long queryTimeoutSec = getQueryTimeoutSec();
        if (queryTimeoutSec >= 0) {
            long runningTimeMsec = System.currentTimeMillis() - getStartTime();
            if (runningTimeMsec > queryTimeoutSec * 1000) {
                throw new QueryTimedOutException(runningTimeMsec);
            }
        }
    }

    @Override
    public void checkConstraints(Row row, boolean usePValues) throws InvalidOperationException {
        row.rowType().constraintChecker().checkConstraints(row, usePValues);
    }
}
