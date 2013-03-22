
package com.akiban.sql.pg;

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
