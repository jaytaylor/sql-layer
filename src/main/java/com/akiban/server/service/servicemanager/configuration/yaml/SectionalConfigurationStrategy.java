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

import com.akiban.server.service.servicemanager.configuration.ServiceBinding;

import java.util.Collection;

public interface SectionalConfigurationStrategy {
    void bind(String interfaceName, String implementingClassName);
    void require(String interfaceName);
    Collection<ServiceBinding> serviceBindings();
    void lock(String interfaceName);
    void mustBeLocked(String interfaceName);
    void mustBeBound(String interfaceName);
    void sectionEnd();
    void unrecognizedCommand(String where, Object command, String message);
}
