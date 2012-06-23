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

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.QueryContext.NotificationLevel;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.util.SparseArray;

import java.util.List;

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
            throw new IllegalArgumentException("no preptime cache objects");
        return preptimeCache.getIfDefined(index);
    }

    public boolean hasExectimeObject(int index) {
        return exectimeCache != null && exectimeCache.isDefined(index);
    }

    public Object exectimeObjectAt(int index) {
        if (exectimeCache == null)
            throw new IllegalArgumentException("no exectime cache objects");
        return exectimeCache.getIfDefined(index);
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

    public String getCurrentTimezone()
    {
        // TODO: This should come from the query context or something of the same nature
        throw new UnsupportedOperationException("not supported yet");
    }

    public String getLocale()
    {
        // TODO:
        throw new UnsupportedOperationException("not supported yet");
    }

    // state

    TExecutionContext(SparseArray<Object> preptimeCache,
                      List<TInstance> inputTypes,
                      TInstance outputType,
                      QueryContext queryContext)
    {
        this.preptimeCache = preptimeCache;
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
    }

    private SparseArray<Object> preptimeCache;
    private SparseArray<Object> exectimeCache;
    private List<TInstance> inputTypes;
    private TInstance outputType;
    private QueryContext queryContext;
}
