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

package com.akiban.sql.pg;

import com.akiban.server.types.AkType;

public class PostgresBoundQueryContext extends PostgresQueryContext 
{
    private PostgresStatement statement;
    private boolean[] columnBinary;
    private boolean defaultColumnBinary;
    
    public PostgresBoundQueryContext(PostgresServerSession server, 
                                     PostgresStatement statement,
                                     Object[] parameters,
                                     boolean[] columnBinary, 
                                     boolean defaultColumnBinary) {
        super(server);
        this.statement = statement;
        this.columnBinary = columnBinary;
        this.defaultColumnBinary = defaultColumnBinary;
        if (parameters != null) {
            boolean usePValues = false;
            if (statement instanceof PostgresBaseStatement)
                usePValues = ((PostgresDMLStatement)statement).usesPValues();
            decodeParameters(parameters, usePValues);
        }
    }

    public PostgresStatement getStatement() {
        return statement;
    }

    public boolean isColumnBinary(int i) {
        if ((columnBinary != null) && (i < columnBinary.length))
            return columnBinary[i];
        else
            return defaultColumnBinary;
    }

    protected void decodeParameters(Object[] parameters, boolean usePValues) {
        PostgresType[] parameterTypes = statement.getParameterTypes();
        for (int i = 0; i < parameters.length; i++) {
            PostgresType pgType = (parameterTypes == null) ? null : parameterTypes[i];
            AkType akType = null;
            if (pgType != null)
                akType = pgType.getAkType();
            if (akType == null)
                akType = AkType.VARCHAR;
            setValue(i, parameters[i], akType, usePValues);
        }
    }

}
