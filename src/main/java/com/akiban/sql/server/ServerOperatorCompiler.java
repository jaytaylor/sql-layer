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

package com.akiban.sql.server;

import com.akiban.server.types3.Types3Switch;
import com.akiban.sql.optimizer.OperatorCompiler;
import com.akiban.sql.optimizer.rule.BaseRule;
import com.akiban.sql.parser.DMLStatementNode;

import com.akiban.server.service.EventTypes;
import com.akiban.server.service.instrumentation.SessionTracer;

public abstract class ServerOperatorCompiler extends OperatorCompiler
{
    protected SessionTracer tracer;

    protected ServerOperatorCompiler() {
    }

    protected void initServer(ServerSession server) {
        initProperties(server.getCompilerProperties());
        initAIS(server.getAIS(), server.getDefaultSchemaName());
        initParser(server.getParser());
        initFunctionsRegistry(server.functionsRegistry());
        if (Boolean.parseBoolean(server.getProperty("cbo", "true"))) {
            boolean usePValues = Types3Switch.ON || Boolean.parseBoolean(server.getProperty("newtypes", "false"));
            initCostEstimator(server.costEstimator(this), usePValues);

        }
        else
            initCostEstimator(null, false);
        
        server.getBinderContext().setBinderAndTypeComputer(binder, typeComputer);

        server.setAttribute("compiler", this);

        tracer = server.getSessionTracer();
    }

    @Override
    protected DMLStatementNode bindAndTransform(DMLStatementNode stmt)  {
        try {
            tracer.beginEvent(EventTypes.BIND_AND_GROUP); // TODO: rename.
            return super.bindAndTransform(stmt);
        } 
        finally {
            tracer.endEvent();
        }
    }

    @Override
    public void beginRule(BaseRule rule) {
        tracer.beginEvent(rule.getTraceName());
    }

    @Override
    public void endRule(BaseRule rule) {
        tracer.endEvent();
    }

}
