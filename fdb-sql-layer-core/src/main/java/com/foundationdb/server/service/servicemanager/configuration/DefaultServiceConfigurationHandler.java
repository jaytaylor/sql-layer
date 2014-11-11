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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class DefaultServiceConfigurationHandler implements ServiceConfigurationHandler {

    // YamlConfigurationStrategy interface

    @Override
    public void bind(String interfaceName, String implementingClassName, ClassLoader classLoader) {
        builder.bind(interfaceName, implementingClassName, classLoader);
    }

    @Override
    public void bindModules(List<Module> modules) {
        if (this.modules == null)
            this.modules = new ArrayList<>(modules.size());
        this.modules.addAll(modules);
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
    public void prioritize(String interfaceName) {
        builder.prioritize(interfaceName);
    }

    @Override
    public void sectionEnd() {
        builder.markSectionEnd();
    }

    @Override
    public void unrecognizedCommand(String where, Object command, String message) {
        throw new ServiceConfigurationException(
                String.format("unrecognized command at %s: %s (%s)",
                        where,
                        message,
                        command
                )
        );
    }

    @Override
    public void bindModulesError(String where, Object command, String message) {
        throw new ServiceConfigurationException(
                String.format("bind-modules error at %s: %s (%s)",
                        where,
                        message,
                        command
                )
        );
    }

    @Override
    public void unbind(String interfaceName) {
        builder.unbind(interfaceName);
    }

    // DefaultServiceConfigurationHandler interface

    public Collection<? extends Module> getModules() {
        Collection<Module> internal = modules == null ? Collections.<Module>emptyList() : modules;
        return Collections.unmodifiableCollection(internal);
    }

    public Collection<ServiceBinding> serviceBindings(boolean strict) {
        return builder.getAllBindings(strict);
    }

    public List<String> priorities() {
        return builder.getPriorities();
    }

    // object state
    private final ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
    private Collection<Module> modules = null;
}
