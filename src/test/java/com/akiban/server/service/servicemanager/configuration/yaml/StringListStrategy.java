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

import java.util.ArrayList;
import java.util.List;

public final class StringListStrategy implements YamlConfigurationStrategy {

    // YamlConfigurationStrategy interface

    @Override
    public void bind(String interfaceName, String implementingClassName) {
        say("BIND %s -> %s", interfaceName, implementingClassName);
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
    public void sectionEnd() {
        say("SECTION END");
    }

    @Override
    public void unrecognizedCommand(String where, Object command) {
        say("ERROR: (%s) %s", where, command);
    }

    // private methods
    private void say(String format, Object... args) {
        strings.add(String.format(format, args));
    }

    // object state
    private final List<String> strings = new ArrayList<String>();
}
