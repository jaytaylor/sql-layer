package com.akiban.sql.pg;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PostgresServerInstrumentedIT extends PostgresServerSelectIT {

    @Before
    public void enableInstrumentation() throws Exception {
        serviceManager().getInstrumentationService().enable();
    }
    
    @After
    public void disableInstrumentation() throws Exception {
        serviceManager().getInstrumentationService().disable();
    }
    
    public PostgresServerInstrumentedIT(String caseName, 
                                        String sql, 
                                        String expected, 
                                        String[] params) {
        super(caseName, sql, expected, params);
    }

}
