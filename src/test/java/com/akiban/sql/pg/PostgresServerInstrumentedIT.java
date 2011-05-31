package com.akiban.sql.pg;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.akiban.server.service.instrumentation.InstrumentationMXBean;
import com.akiban.sql.TestBase;

@RunWith(Parameterized.class)
public class PostgresServerInstrumentedIT extends PostgresServerITBase {
    
    public static final File RESOURCE_DIR = 
        new File(PostgresServerITBase.RESOURCE_DIR, "select");

    @Before
    // Note that this runs _after_ super's openTheConnection(), which
    // means that there is always an AIS generation flush.
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    @Parameters
    public static Collection<Object[]> queries() throws Exception {
        return TestBase.sqlAndExpectedAndParams(RESOURCE_DIR);
    }

    public PostgresServerInstrumentedIT(String caseName, 
                                        String sql, 
                                        String expected, 
                                        String[] params) {
        super(caseName, sql, expected, params);
        //InstrumentationMXBean mxbean = (InstrumentationMXBean) serviceManager().getInstrumentationService();
        //mxbean.enable();
    }

    @Test
    public void testQuery() throws Exception {
        StringBuilder data = new StringBuilder();
        PreparedStatement stmt = connection.prepareStatement(sql);
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String param = params[i];
                if (param.startsWith("#"))
                    stmt.setLong(i + 1, Long.parseLong(param.substring(1)));
                else
                    stmt.setString(i + 1, param);
            }
        }
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            if (i > 1) data.append('\t');
            data.append(md.getColumnName(i));
        }
        data.append('\n');
        while (rs.next()) {
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) data.append('\t');
                data.append(rs.getString(i));
            }
            data.append('\n');
        }
        stmt.close();
        assertEquals("Difference in " + caseName, expected, data.toString());
    }

}
