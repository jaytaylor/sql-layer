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

import com.akiban.server.service.servicemanager.configuration.BindingConfiguration;

interface YamlConfigurationStrategy extends BindingConfiguration {
    // There is not currently a need for an intermediate LockableBindingConfiguration. If we start needing that,
    // we can add it and pull lock there.
    void lock(String interfaceName);

    // The following are essentially checks, which rely on the concept of a configuration section. BindingConfiguration
    // doesn't have that concept, which is why these aren't there (even though mustBeBound could, in theory)
    void mustBeLocked(String interfaceName);
    void mustBeBound(String interfaceName);
    void sectionEnd();
    void unrecognizedCommand(String where, Object command, String message);
}
