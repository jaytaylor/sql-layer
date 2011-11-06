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

package com.akiban.sql.pg;

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;

import com.akiban.qp.operator.Bindings;

import java.util.List;
import java.io.IOException;

import com.akiban.qp.operator.UndefBindings;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.session.Session;
import com.akiban.util.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.akiban.server.expression.std.EnvironmentExpression.EnvironmentValue;

import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction.*;

/**
 * An SQL modifying DML statement transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresModifyOperatorStatement extends PostgresBaseStatement
{
    private String statementType;
    private UpdatePlannable resultOperator;

    private static final Tap.InOutTap EXECUTE_TAP = Tap.createTimer("PostgresBaseStatement: execute exclusive");
    private static final Tap.InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresBaseStatement: acquire exclusive lock");
    private static final Logger LOG = LoggerFactory.getLogger(PostgresModifyOperatorStatement.class);
        
    public PostgresModifyOperatorStatement(String statementType,
                                           UpdatePlannable resultOperator,
                                           PostgresType[] parameterTypes,
                                           List<EnvironmentValue> environmentValues) {
        super(parameterTypes, environmentValues);
        this.statementType = statementType;
        this.resultOperator = resultOperator;
    }
    
    public int execute(PostgresServerSession server, int maxrows)
        throws IOException {

        PostgresMessenger messenger = server.getMessenger();
        Bindings bindings = getBindings();
        Session session = server.getSession();
        final UpdateResult updateResult;
        try {
            lock(session, UNSPECIFIED_DML_WRITE);
            setEnvironmentBindings(server, bindings);
            updateResult = resultOperator.run(bindings, server.getStore());
        } finally {
            unlock(session, UNSPECIFIED_DML_WRITE);
        }

        LOG.debug("Statement: {}, result: {}", statementType, updateResult);
        
        messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
        //TODO: Find a way to extract InsertNode#statementToString() or equivalent
        if (statementType.equals("INSERT")) {
            messenger.writeString(statementType + " 0 " + updateResult.rowsModified());
        } else {
            messenger.writeString(statementType + " " + updateResult.rowsModified());
        }
        messenger.sendMessage();
        return 0;
    }

    @Override
    protected Tap.InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    protected Tap.InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }

    /** Only needed in the case where a statement has parameters. */
    static class BoundStatement extends PostgresModifyOperatorStatement {
        private Bindings bindings;
        private int nparams;

        public BoundStatement(String statementType,
                              UpdatePlannable resultOperator,
                              Bindings bindings, int nparams,
                              List<EnvironmentValue> environmentValues) {
            super(statementType, resultOperator, null, environmentValues);
            this.bindings = bindings;
            this.nparams = nparams;
        }

        @Override
        public Bindings getBindings() {
            return bindings;
        }

        @Override
        protected int getNParameters() {
            return nparams;
        }
    }

    /** Get a bound version of a predicate by applying given parameters. */
    @Override
    public PostgresStatement getBoundStatement(String[] parameters,
                                               boolean[] columnBinary, 
                                               boolean defaultColumnBinary) {
        if (parameters == null)
            return this;        // Can be reused.

        return new BoundStatement(statementType, resultOperator, 
                                  getParameterBindings(parameters), parameters.length,
                                  getEnvironmentValues());
    }
}
