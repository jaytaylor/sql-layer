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

package com.foundationdb.server.service.servicemanager.configuration.yaml;

import com.foundationdb.server.service.servicemanager.configuration.ServiceConfigurationHandler;
import com.google.inject.Module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class StringListConfigurationHandler implements ServiceConfigurationHandler {

    // YamlConfigurationStrategy interface

    @Override
    public void bind(String interfaceName, String implementingClassName, ClassLoader ignored) {
        say("BIND %s -> %s", interfaceName, implementingClassName);
    }

    @Override
    public void bindModules(List<Module> modules) {
        for (Module module : modules)
            say("BIND-MODULE %s", module.getClass());
    }

    @Override
    public void lock(String interfaceName) {
        say("LOCK %s", interfaceName);
    }

    @Override
    public void require(String interfaceName) {
        say("REQUIRE %s", interfaceName);
    }

    @Override
    public void mustBeLocked(String interfaceName) {
        say("MUST BE LOCKED %s", interfaceName);
    }

    @Override
    public void mustBeBound(String interfaceName) {
        say("MUST BE BOUND %s", interfaceName);
    }

    @Override
    public void prioritize(String interfaceName) {
        say("PRIORITIZE %s", interfaceName);
    }

    @Override
    public void sectionEnd() {
        say("SECTION END");
    }

    @Override
    public void unrecognizedCommand(String where, Object command, String message) {
        say("ERROR: %s (at %s) %s", message, where, command);
    }

    @Override
    public void bindModulesError(String where, Object command, String message) {
        say("BIND-MODULES ERROR: %s (at %s) %s", message, where, command);
    }

    @Override
    public void unbind(String interfaceName) {
        say("UNBIND %s", interfaceName);
    }

    // StringListStrategy interface

    public List<String> strings() {
        return unmodifiableStrings;
    }

    // private methods
    private void say(String format, Object... args) {
        strings.add(String.format(format, args));
    }

    // object state
    private final List<String> strings = new ArrayList<>();
    private final List<String> unmodifiableStrings = Collections.unmodifiableList(strings);
}
