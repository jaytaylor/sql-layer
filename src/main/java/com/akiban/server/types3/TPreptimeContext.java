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
import com.akiban.util.SparseArray;

import java.util.List;
import java.util.TimeZone;

public final class TPreptimeContext {
    public List<TInstance> getInputTypes() {
        return inputTypes;
    }

    public TInstance inputTypeAt(int index) {
        return inputTypes.get(index);
    }

    public TInstance getOutputType() {
        return outputType;
    }

    public void setOutputType(TInstance outputType) {
        this.outputType = outputType;
    }

    public TExecutionContext createExecutionContext() {
        return new TExecutionContext(preptimeCache, inputTypes, outputType, 
                queryContext,
                null, null, null); // TODO pass in
    }
    
    public String getCurrentTimezone()
    {
        // TODO need to get this from the session
        return TimeZone.getDefault().getID();
    }
    
    public String getLocale()
    {
        // TODO:
        throw new UnsupportedOperationException("not supported yet");
    }
    
    public Object get(int index) {
        if ((preptimeCache != null) && preptimeCache.isDefined(index))
            return preptimeCache.get(index);
        else
            return null;
    }

    public void set(int index, Object value) {
        if (preptimeCache == null)
            preptimeCache = new SparseArray<Object>(index);
        preptimeCache.set(index, value);
    }

    public SparseArray<Object> getValues() {
        return preptimeCache;
    }

    public TPreptimeContext(List<TInstance> inputTypes, QueryContext queryContext) {
        this.inputTypes = inputTypes;
        this.queryContext = queryContext;
    }

    public TPreptimeContext(List<TInstance> inputTypes, TInstance outputType, QueryContext queryContext) {
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
    }

    public TPreptimeContext(List<TInstance> inputTypes, TInstance outputType, QueryContext queryContext, SparseArray<Object> preptimeCache) {
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
        this.preptimeCache = preptimeCache;
    }

    private final QueryContext queryContext;
    private SparseArray<Object> preptimeCache;
    private List<TInstance> inputTypes;
    private TInstance outputType;
}
