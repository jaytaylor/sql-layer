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

import org.yaml.snakeyaml.Yaml;

import java.io.Reader;

@SuppressWarnings("unused")
public final class YamlConfiguration {

    // YamlConfiguration interface

    public void read(Reader source) {
        Object parsed = parser.load(source);
        throw new UnsupportedOperationException();
    }

    public YamlConfiguration() {
        this(new RealYamlConfiguraitonStrategy());
    }

    YamlConfiguration(YamlConfigurationStrategy strategy) {
        this.strategy = strategy;
    }

    // internal state
    private final YamlConfigurationStrategy strategy;
    private final Yaml parser = new Yaml();
}
