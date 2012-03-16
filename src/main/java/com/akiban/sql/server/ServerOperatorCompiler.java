/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.server;

import com.akiban.sql.StandardException;
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
        if ("true".equals(server.getProperty("cbo")))
            initCostEstimator(server.costEstimator(this));
        else
            initCostEstimator(null);
        
        server.setAttribute("aisBinder", binder);
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
