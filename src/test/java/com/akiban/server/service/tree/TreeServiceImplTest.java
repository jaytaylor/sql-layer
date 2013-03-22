
package com.akiban.server.service.tree;

import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import com.akiban.server.service.config.TestConfigService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;

public class TreeServiceImplTest {

    private final static int MEGA = 1024 * 1024;

    private TestConfigService configService;

    @Before
    public void startConfiguration() throws Exception {
        configService = new TestConfigService();
        configService.start();
    }

    @After
    public void stopConfiguration() throws Exception {
        configService.start();
    }

    @Test
    public void startupPropertiesTest() throws Exception {
        final Properties properties = TreeServiceImpl.setupPersistitProperties(configService);
        assertNotNull(properties.getProperty("datapath"));
        assertNotNull(properties.getProperty("buffer.memory.16384"));
    }
    
}
