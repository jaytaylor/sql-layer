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

package com.foundationdb.sql.pg;

import com.foundationdb.sql.pg.PostgresEmulatedMetaDataStatement.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Handle known system table queries from tools directly.  At some
 * point it may be possible to actually implement <code>pg_</code>
 * tables as views against the SQL Layer's own information schema. But for
 * now, some of the queries do not even parse in our dialect.
 */
public class PostgresEmulatedMetaDataStatementParser implements PostgresStatementParser
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresEmulatedMetaDataStatementParser.class);

    /** Quickly determine whether a given query <em>might</em> be a
     * Postgres system table. */
    public static final String POSSIBLE_PG_QUERY = "FROM\\s+PG_|PG_CATALOG\\.|PG_IS_";
    
    private Pattern possiblePattern;

    public PostgresEmulatedMetaDataStatementParser(PostgresServerSession server) {
        possiblePattern = Pattern.compile(POSSIBLE_PG_QUERY, Pattern.CASE_INSENSITIVE);
    }

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes)  {
        if (!possiblePattern.matcher(sql).find())
            return null;
        List<String> groups = new ArrayList<>();
        for (Query query : Query.values()) {
            if (query.matches(sql, groups)) {
                logger.debug("Emulated: {}{}", query, groups.subList(1, groups.size()));
                return new PostgresEmulatedMetaDataStatement(query, groups,
                                                             server.typesTranslator());
            }
        }
        return null;
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
    }

}
