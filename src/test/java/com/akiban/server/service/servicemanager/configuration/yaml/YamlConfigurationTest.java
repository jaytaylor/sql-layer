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

package com.akiban.server.service.servicemanager.configuration.yaml;

import com.akiban.util.Strings;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;

public final class YamlConfigurationTest {
    @Test
    public void sandbox() throws Exception {
        StringListStrategy strategy = new StringListStrategy();
        YamlConfiguration configuration = new YamlConfiguration(strategy);

        Reader reader = new InputStreamReader(YamlConfigurationTest.class.getResourceAsStream("simple-1.yaml"), "UTF-8");
        try {
            configuration.read(reader);
        } finally {
            reader.close();
        }
    }
}
