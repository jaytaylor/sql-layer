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

    private static class MyConfigService extends TestConfigService {
    }

    private Service<ConfigurationService> configService;

    @Before
    public void startConfiguration() throws Exception {
        configService = new MyConfigService();
        configService.start();
    }

    @After
    public void stopConfiguration() throws Exception {
        configService.start();
    }

    @Test
    public void startupPropertiesTest() throws Exception {
        final Properties properties = TreeServiceImpl.setupPersistitProperties(configService.cast());
        assertNotNull(properties.getProperty("datapath"));
        assertNotNull(properties.getProperty("buffer.memory.16384"));
    }
    
}
