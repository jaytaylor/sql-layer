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

import com.akiban.sql.pg.PostgresEmulatedMetaDataStatement.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/** Handle known system table queries from tools directly.  At some
 * point it may be possible to actually implement <code>pg_</code>
 * tables as views against Akiban's own information schema. But for
 * now, some of the queries do not even parse in Akiban SQL.
 */
public class PostgresEmulatedMetaDataStatementParser implements PostgresStatementParser
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresEmulatedMetaDataStatementParser.class);

    /** Quickly determine whether a given query <em>might</em> be a
     * Postgres system table. */
    public static final String POSSIBLE_PG_QUERY = "FROM\\s+PG_";
    
    private Pattern possiblePattern;

    public PostgresEmulatedMetaDataStatementParser(PostgresServerSession server) {
        possiblePattern = Pattern.compile(POSSIBLE_PG_QUERY, Pattern.CASE_INSENSITIVE);
    }

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes)  {
        if (!possiblePattern.matcher(sql).find())
            return null;
        for (Query query : Query.values()) {
            if (sql.equalsIgnoreCase(query.getSQL())) {
                logger.debug("Emulated: {}", query);
                return new PostgresEmulatedMetaDataStatement(query);
            }
        }
        return null;
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
    }

}
