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

import com.foundationdb.sql.pg.PostgresEmulatedSessionStatement.Verb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handle session statements that are not worth cluttering up the grammar with.
 */
public class PostgresEmulatedSessionStatementParser implements PostgresStatementParser
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresEmulatedSessionStatementParser.class);

    private final int possibleLetters;

    public PostgresEmulatedSessionStatementParser(PostgresServerSession server) {
        int letters = 0;
        for (Verb verb : Verb.values()) {
            letters |= (1 << (verb.getSQL().charAt(0) - 'A'));
        }
        possibleLetters = letters;
    }

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes)  {
        if (sql.length() == 0)
            return null;
        char ch = sql.charAt(0);
        if ((ch >= 'A') && (ch <= 'Z')) {
            if ((possibleLetters & (1 << (ch - 'A'))) == 0)
                return null;
        }
        else if ((ch >= 'a') && (ch <= 'z')) {
            if ((possibleLetters & (1 << (ch - 'a'))) == 0)
                return null;
        }
        else
            return null;
        // First alpha word terminated by EOF / SP / ;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sql.length(); i++) {
            ch = sql.charAt(i);
            if ((ch == ' ') || (ch == ';'))
                break;
            if (!(((ch >= 'A') && (ch <= 'Z')) || ((ch >= 'a') && (ch <= 'z'))))
                return null;
            sb.append(ch);
        }
        String str = sb.toString();
        for (Verb verb : Verb.values()) {
            if (verb.getSQL().equalsIgnoreCase(str)) {
                logger.debug("Emulated: {}", verb);
                return new PostgresEmulatedSessionStatement(verb, sql);
            }
        }
        return null;
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
    }

}
