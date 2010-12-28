package com.akiban.ais.ddl;

import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.RowDefCacheFactory;
import static junit.framework.Assert.*;
import org.junit.Test;

public class GrammarTest
{
    // from bug695066
    @Test
    public void testUniqueKeyInColumnDeclaration() throws Exception
    {
        test("CREATE TABLE t1(c1 TINYINT AUTO_INCREMENT NULL UNIQUE KEY) AUTO_INCREMENT=10;");
    }

    // from bug695066
    @Test
    public void testDECColumns() throws Exception
    {
        test("CREATE TABLE t1(c1 DEC NULL, c2 DEC NULL);");
    }

    private void test(String declaration)
    {
        String ddl = String.format("use schema; %s", declaration);
        try {
            ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        } catch (Exception e) {
            assertTrue(false);
        }
    }

    private static final RowDefCacheFactory ROW_DEF_CACHE_FACTORY = new RowDefCacheFactory();
}
