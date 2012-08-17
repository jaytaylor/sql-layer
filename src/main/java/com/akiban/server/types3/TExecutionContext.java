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

package com.akiban.server.types3;

import com.akiban.ais.model.TableName;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.QueryContext.NotificationLevel;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.OverflowException;
import com.akiban.server.error.StringTruncationException;
import com.akiban.util.SparseArray;
import com.google.common.base.Objects;

import java.util.List;
import java.util.TimeZone;

public final class TExecutionContext {

    public TInstance inputTInstanceAt(int index) {
        return inputTypes.get(index);
    }

    public Object objectAt(int index) {
        Object result = null;
        if (preptimeCache != null && preptimeCache.isDefined(index))
            result = preptimeCache.get(index);
        if (result == null && exectimeCache != null && exectimeCache.isDefined(index))
            result = exectimeCache.get(index);
        return result;
    }

    public TInstance outputTInstance() {
        return outputType;
    }

    public Object preptimeObjectAt(int index) {
        if (preptimeCache == null)
            preptimeCache = new SparseArray<Object>(index);
        return preptimeCache.getIfDefined(index);
    }

    public boolean hasExectimeObject(int index) {
        return exectimeCache != null && exectimeCache.isDefined(index);
    }

    public Object exectimeObjectAt(int index) {
        if (exectimeCache == null)
            exectimeCache = new SparseArray<Object>();
        return exectimeCache.get(index);
    }

    public void putExectimeObject(int index, Object value) {
        if (preptimeCache != null && preptimeCache.isDefined(index)) {
            Object conflict = preptimeCache.get(index);
            throw new IllegalStateException("conflicts with preptime value: " + conflict);
        }
        if (exectimeCache == null)
            exectimeCache = new SparseArray<Object>(index);
        exectimeCache.set(index, value);
    }

    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        queryContext.notifyClient(level, errorCode, message);
    }

    public void warnClient(InvalidOperationException exception) {
        queryContext.warnClient(exception);
    }

    public String getCurrentLocale()
    {
        throw new UnsupportedOperationException("getLocale() not supported yet");
    }

    /**
     * Some functions need to get the current timezone (session/global), not the JVM's timezone.
     * @return  the server's timezone.
     */
    public String getCurrentTimezone()
    {
        // TODO need to get this from the session
        return TimeZone.getDefault().getID();
    }

    /**
     * 
     * @return  the time at which the query started
     */
    public long getCurrentDate()
    {
        return queryContext.getStartTime();
    }
    
    public String getCurrentUser()
    {
        return queryContext.getCurrentUser();
    }
    
    public String getSessionUser()
    {
        return queryContext.getSessionUser();
    }
    
    public String getSystemUser()
    {
        return queryContext.getSystemUser();
    }
    
    public long sequenceNextValue (TableName sequenceName) 
    {
        return queryContext.sequenceNextValue(sequenceName);
    }

    public long sequenceCurrentValue (TableName sequenceName) 
    {
        return queryContext.sequenceCurrentValue(sequenceName);
    }
    
    public void reportOverflow(String msg)
    {
        switch(overflowHandling)
        {
            case WARN:
                warnClient(new OverflowException());
                break;
            case ERROR:
                throw new OverflowException();
            case IGNORE:
                // ignores, does nothing
                break;
            default:
                throw new AssertionError(overflowHandling);
        }
    }
    
    public void reportTruncate(String original, String truncated)
    {
        switch(truncateHandling)
        {
            case WARN:
                warnClient(new StringTruncationException(original, truncated));
                break;
            case ERROR:
                throw new StringTruncationException(original, truncated);
            case IGNORE:
                // ignores, does nothing
                break;
            default:
                throw new AssertionError(truncateHandling);
        }
    }
    
    public void reportBadValue(String msg)
    {
        switch(invalidFormatHandling)
        {
            case WARN:
                warnClient(new InvalidParameterValueException(msg));
                break;
            case ERROR:
                throw new InvalidParameterValueException(msg);
            case IGNORE:
                // ignores, does nothing
                break;
            default:
                throw new AssertionError(invalidFormatHandling);
        }
    }

    public void setQueryContext(QueryContext queryContext) {
        this.queryContext = queryContext;
    }

    public TExecutionContext deriveContext(List<TInstance> inputTypes, TInstance outputType) {
        return new TExecutionContext(
                new SparseArray<Object>(),
                inputTypes,
                outputType,
                queryContext,
                overflowHandling,
                truncateHandling,
                invalidFormatHandling
        );
    }

    // state

    public TExecutionContext(List<TInstance> inputTypes,  TInstance outputType, QueryContext queryContext) {
        this(null, inputTypes, outputType, queryContext, null, null, null);
    }

    public TExecutionContext(SparseArray<Object> preptimeCache,
                      List<TInstance> inputTypes,
                      TInstance outputType,
                      QueryContext queryContext,
                      ErrorHandlingMode overflow,
                      ErrorHandlingMode truncate,
                      ErrorHandlingMode invalid)
    {
        this.preptimeCache = preptimeCache;
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
        overflowHandling = Objects.firstNonNull(overflow, ErrorHandlingMode.WARN);
        truncateHandling = Objects.firstNonNull(truncate, ErrorHandlingMode.WARN);
        invalidFormatHandling = Objects.firstNonNull(invalid,  ErrorHandlingMode.WARN);
    }

    private SparseArray<Object> preptimeCache;
    private SparseArray<Object> exectimeCache;
    private List<TInstance> inputTypes;
    private TInstance outputType;
    private QueryContext queryContext;
    private ErrorHandlingMode overflowHandling;
    private ErrorHandlingMode truncateHandling;
    private ErrorHandlingMode invalidFormatHandling;
}
