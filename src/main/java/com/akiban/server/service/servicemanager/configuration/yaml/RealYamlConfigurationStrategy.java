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

import com.akiban.server.service.servicemanager.configuration.LockableServiceBindingsBuilder;

public final class RealYamlConfigurationStrategy implements YamlConfigurationStrategy {

    // YamlConfigurationStrategy interface

    @Override
    public void bind(String interfaceName, String implementingClassName) {
        builder.bind(interfaceName, implementingClassName);
    }

    @Override
    public void lock(String interfaceName) {
        builder.lock(interfaceName);
    }

    @Override
    public void require(String interfaceName) {
        builder.markDirectlyRequired(interfaceName);
    }

    @Override
    public void mustBeLocked(String interfaceName) {
        builder.mustBeLocked(interfaceName);
    }

    @Override
    public void mustBeBound(String interfaceName) {
        builder.mustBeBound(interfaceName);
    }

    @Override
    public void sectionEnd() {
        builder.markSectionEnd();
    }

    @Override
    public void unrecognizedCommand(String where, Object command) {
        throw new YamlConfigurationException("unrecognized command at " + where + ": " + command);
    }

    // object state
    private final LockableServiceBindingsBuilder builder = new LockableServiceBindingsBuilder();
}
