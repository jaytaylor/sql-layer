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

package com.foundationdb.server.service.tree;

import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import com.foundationdb.server.service.config.TestConfigService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;

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
