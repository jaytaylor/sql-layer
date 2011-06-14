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
package com.akiban.server.service.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import com.akiban.server.AkServerUtil;
import com.akiban.server.service.Service;
import com.akiban.server.service.UnitTestServiceFactory.TestConfigService;
import com.akiban.server.service.config.ConfigurationService;

public class TreeServiceImplTest {

    private static class MyConfigService extends TestConfigService {
        private MyConfigService() {
            super(null);
        }
    }
    
    private ConfigurationService asConfigService;
    private Service<ConfigurationService> asService;
    
    @SuppressWarnings("unchecked")
    private void createConfiguration() {
        final ConfigurationService configService = new MyConfigService();
        asConfigService = configService;
        asService = (Service<ConfigurationService>) configService;
    }
    
    @Test
    public void bufferMemoryTest() throws Exception {
        final TreeServiceImpl treeService = new TreeServiceImpl();
        final long available = AkServerUtil.availableMemory();
        final long allocation = treeService.bufferMemory(false);
        assertTrue(allocation > (available - TreeServiceImpl.DEFAULT_MEMORY_RESERVATION) / 2);
        assertTrue(allocation < available - TreeServiceImpl.DEFAULT_MEMORY_RESERVATION);

        assertEquals(TreeServiceImpl.DEFAULT_BUFFER_MEMORY,
                treeService.bufferMemory(true));
    }

    @Test
    public void startupPropertiesTest() throws Exception {
        createConfiguration();
        asService.start();
        final TreeServiceImpl treeService = new TreeServiceImpl();
        final Properties properties = treeService
                .setupPersistitProperties(asConfigService);
        assertNotNull(properties.getProperty("datapath"));
        assertNotNull(properties.getProperty("buffer.memory.16384"));
        asService.stop();
    }

}
