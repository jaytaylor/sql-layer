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
