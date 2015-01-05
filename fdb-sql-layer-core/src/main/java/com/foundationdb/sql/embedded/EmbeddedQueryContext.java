/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.embedded;

import com.foundationdb.sql.server.ServerQueryContext;

import com.foundationdb.server.error.ErrorCode;

public class EmbeddedQueryContext extends ServerQueryContext<JDBCConnection>
{
    private JDBCStatement statement;
    private JDBCResultSet resultSet;

    protected EmbeddedQueryContext(JDBCConnection connection) {
        super(connection);
    }

    protected EmbeddedQueryContext(JDBCStatement statement) {
        super(statement.connection);
        this.statement = statement;
    }

    protected EmbeddedQueryContext(JDBCResultSet resultSet) {
        super(resultSet.statement.connection);
        this.statement = resultSet.statement;
        this.resultSet = resultSet;
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        if (getServer().shouldNotify(level)) {
            JDBCWarning warning = new JDBCWarning(level, errorCode, message);
            // If we are associated with a particular result set /
            // statement, direct warning there.
            if (resultSet != null)
                resultSet.addWarning(warning);
            else if (statement != null)
                statement.addWarning(warning);
            else
                getServer().addWarning(warning);
        }
    }

}
