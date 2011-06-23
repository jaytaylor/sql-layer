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
import com.akiban.sql.StandardException;

import com.akiban.qp.physicaloperator.BindingNotSetException;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.IncompatibleRowException;
import com.akiban.qp.physicaloperator.StoreAdapterRuntimeException;
import com.akiban.qp.physicaloperator.CursorUpdateException;
import com.akiban.qp.physicaloperator.UndefBindings;

import java.util.*;
import java.io.IOException;

/**
 * An SQL modifying DML statement transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresModifyOperatorStatement extends PostgresBaseStatement
{
    private String statementType;
    private UpdatePlannable resultOperator;
        
    public PostgresModifyOperatorStatement(String statementType,
                                           UpdatePlannable resultOperator,
                                           PostgresType[] parameterTypes) {
        super(parameterTypes);
        this.statementType = statementType;
        this.resultOperator = resultOperator;
    }
    
    public void execute(PostgresServerSession server, int maxrows)
        throws IOException, StandardException {
        PostgresMessenger messenger = server.getMessenger();
        Bindings bindings = getBindings();
        UpdateResult updateResult;
        try {
            updateResult = resultOperator.run(bindings, server.getStore());
        }
        catch (BindingNotSetException ex) {
            throw new StandardException(ex);
        }
        catch (IncompatibleRowException ex) {
            throw new StandardException(ex);
        }
        catch (StoreAdapterRuntimeException ex) {
            throw new StandardException(ex);
        }
        catch (CursorUpdateException ex) {
            throw new StandardException(ex);
        }
        {        
            messenger.beginMessage(PostgresMessenger.COMMAND_COMPLETE_TYPE);
            messenger.writeString(statementType + " " + updateResult.rowsModified());
            messenger.sendMessage();
        }
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
                                               boolean defaultColumnBinary) 
            throws StandardException {
        if (parameters == null)
            return this;        // Can be reused.

        return new BoundStatement(statementType, resultOperator, 
                                  getParameterBindings(parameters));
    }

}
