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
import com.akiban.qp.operator.UndefBindings;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An SQL modifying DML statement transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresModifyOperatorStatement extends PostgresBaseStatement
{
    private String statementType;
    private UpdatePlannable resultOperator;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresModifyOperatorStatement.class);
        
    public PostgresModifyOperatorStatement(String statementType,
                                           UpdatePlannable resultOperator,
                                           PostgresType[] parameterTypes) {
        super(parameterTypes);
        this.statementType = statementType;
        this.resultOperator = resultOperator;
    }
    
    public int execute(PostgresServerSession server, int maxrows)
        throws IOException {

        PostgresMessenger messenger = server.getMessenger();
        Bindings bindings = getBindings();
        final UpdateResult updateResult = resultOperator.run(bindings, server.getStore());
        
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

    protected Bindings getBindings() {
        return UndefBindings.only();
    }

    /** Only needed in the case where a statement has parameters. */
    static class BoundStatement extends PostgresModifyOperatorStatement {
        private Bindings bindings;

        public BoundStatement(String statementType,
                              UpdatePlannable resultOperator,
                              Bindings bindings) {
            super(statementType, resultOperator, null);
            this.bindings = bindings;
        }

        @Override
        public Bindings getBindings() {
            return bindings;
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
                                  getParameterBindings(parameters));
    }

}
