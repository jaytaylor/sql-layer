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

import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

public class SortTimingIT extends PostgresServerITBase
{
    private static final int N = 300000;
    private static final int FILLER_SIZE = 100;
    private static final String FILLER;
    static
    {
        char[] filler = new char[FILLER_SIZE];
        Arrays.fill(filler, 'x');
        FILLER = new String(filler);
    }

    @Test
    @Ignore
    public void test() throws Exception
    {
        loadDB();
        sort(false);
        sort(true);
    }

    private void loadDB() throws Exception
    {
        Statement statement = getConnection().createStatement();
        statement.execute(
            String.format("create table t(id integer not null primary key, foobar int, filler varchar(%s))",
                          FILLER_SIZE));
        Random random = new Random();
        for (int id = 0; id < N; id++) {
            statement.execute(String.format("insert into t values(%s, %s, '%s')", id, random.nextInt(), FILLER));
        }
        statement.execute("select count(*) from t");
        ResultSet resultSet = statement.getResultSet();
        resultSet.next();
        System.out.println(String.format("Loaded %s rows", resultSet.getInt(1)));
        statement.close();
    }

    private void sort(boolean tempVolume) throws Exception
    {
        long start = System.currentTimeMillis();
        System.setProperty("sorttemp", tempVolume ? "true" : "false");
        Statement statement = getConnection().createStatement();
        statement.execute("select * from t order by foobar");
        ResultSet resultSet = statement.getResultSet();
        resultSet.next();
        long stop = System.currentTimeMillis();
        System.out.println(String.format("sort with temp = %s: %s msec", tempVolume, (stop - start)));
        resultSet.close();
        statement.close();
    }
}
