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

package com.foundationdb.server.service.servicemanager.configuration;

import com.google.inject.Module;

import java.util.List;

public interface ServiceConfigurationHandler {
    void bind(String interfaceName, String implementingClassName, ClassLoader classLoader);
    void bindModules(List<Module> modules);
    void require(String interfaceName);
    void lock(String interfaceName);
    void mustBeLocked(String interfaceName);
    void mustBeBound(String interfaceName);
    void prioritize(String interfaceName);
    void sectionEnd();
    void unrecognizedCommand(String where, Object command, String message);
    void bindModulesError(String where, Object command, String message);
    void unbind(String interfaceName);
}
