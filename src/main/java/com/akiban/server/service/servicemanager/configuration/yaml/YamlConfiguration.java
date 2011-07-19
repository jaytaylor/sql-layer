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

import com.akiban.server.service.servicemanager.configuration.BindingsConfigurationLoader;
import com.akiban.server.service.servicemanager.configuration.ServiceConfigurationHandler;
import com.akiban.util.Enumerated;
import com.akiban.util.EnumeratingIterator;
import org.yaml.snakeyaml.Yaml;

import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public final class YamlConfiguration implements BindingsConfigurationLoader {

    // BindingsConfigurationLoader interface

    @Override
    public void loadInto(ServiceConfigurationHandler config) {
        try {
            Yaml parser = new Yaml();
            for (Enumerated<Object> enumerated : EnumeratingIterator.of(parser.loadAll(source))) {
                Object item = enumerated.get();
                if ( ! (item instanceof List) ) {
                    throw new BadConfigurationException("block " + enumerated.count() + " of " + sourceName, item);
                }
                readBlock(config, enumerated.count(), (List<?>)item);
            }
            config.sectionEnd();
        } catch (BadConfigurationException e) {
            config.unrecognizedCommand(e.getWhere(), e.getObject(), e.getMessage());
        }
    }

    public YamlConfiguration(String sourceName, Reader source) {
        this.sourceName = sourceName;
        this.source = source;
    }

    // private methods

    private void readBlock(ServiceConfigurationHandler config, int blockId, List<?> commands) {
        for (Enumerated<?> enumerated : EnumeratingIterator.of(commands)) {
            Object elem = enumerated.get();
            if ( !(elem instanceof Map)) {
                throw new BadConfigurationException("block " + blockId + " item " + enumerated.count(), elem);
            }
            String where = "block " + blockId + " command " + enumerated.count();
            Map<?,?> commandMap = (Map<?,?>) elem;
            if (commandMap.size() != 1) {
                throw new BadConfigurationException(where, commandMap, "command map needs to have size 1");
            }
            Map.Entry<?,?> commandEntry = commandMap.entrySet().iterator().next();
            Object commandKey = commandEntry.getKey();
            if (! (commandKey instanceof String)) {
                throw new BadConfigurationException(where, commandMap,
                        "command key needs to be a string");
            }
            Command command = whichCommand(where, (String)commandKey);
            Object commandValue = commandEntry.getValue();
            switch (command) {
            case BIND:
                internalDoBind( config, stringsMap(where, commandValue) );
                break;
            case BIND_AND_LOCK:
                internalDoBindAndLock( config, stringsMap(where, commandValue) );
                break;
            case LOCK:
                internalDoLock( config, strings(where, commandValue) );
                break;
            case REQUIRE:
                internalDoRequire( config, strings(where, commandValue) );
                break;
            case REQUIRE_LOCKED:
                internalDoRequireLocked( config, strings(where, commandValue) );
                break;
            case LOCKED:
                internalDoLocked( config, strings(where, commandValue) );
                break;
            case BOUND:
                internalDoBound( config, strings(where, commandValue) );
                break;
            default:
                throw new UnsupportedOperationException(command.name());
            }
        }
    }

    private void internalDoBind(ServiceConfigurationHandler config, Map<String,String> bindings) {
        for(Map.Entry<String,String> binding : bindings.entrySet()) {
            config.bind(binding.getKey(), binding.getValue());
        }
    }

    private void internalDoBindAndLock(ServiceConfigurationHandler config, Map<String,String> bindings) {
        for(Map.Entry<String,String> binding : bindings.entrySet()) {
            config.bind(binding.getKey(), binding.getValue());
            config.lock(binding.getKey());
        }
    }

    private void internalDoLock(ServiceConfigurationHandler config, List<String> interfaceNames) {
        for (String interfaceName : interfaceNames) {
            config.lock(interfaceName);
        }
    }

    private void internalDoRequire(ServiceConfigurationHandler config, List<String> interfaceNames) {
        for (String interfaceName : interfaceNames) {
            config.require(interfaceName);
        }
    }

    private void internalDoRequireLocked(ServiceConfigurationHandler config, List<String> interfaceNames) {
        for (String interfaceName : interfaceNames) {
            config.require(interfaceName);
            config.mustBeLocked(interfaceName);
        }
    }

    private void internalDoLocked(ServiceConfigurationHandler config, List<String> interfaceNames) {
        for (String interfaceName : interfaceNames) {
            config.mustBeLocked(interfaceName);
        }
    }

    private void internalDoBound(ServiceConfigurationHandler config, List<String> interfaceNames) {
        for (String interfaceName : interfaceNames) {
            config.mustBeBound(interfaceName);
        }
    }

    private static Command whichCommand(String where, String commandName) {
        commandName = commandName.toUpperCase().replace(' ', '_');
        try {
            return Command.valueOf(commandName);
        } catch (IllegalArgumentException e) {
            throw new BadConfigurationException(where, commandName);
        }
    }

    private static List<String> strings(String where, Object object) {
        if (object instanceof String) {
            return Collections.singletonList((String)object);
        }
        if (object instanceof List) {
            List<?> wildList = (List<?>) object;
            for (Object elem : wildList) {
                if (!(elem instanceof String)) {
                    throw new BadConfigurationException(where, object, "required a String or List<String>");
                }
            }
            return launderStringsList(wildList);
        }
        throw new BadConfigurationException(where, object, "required a String or List<String>");
    }

    private static Map<String,String> stringsMap(String where, Object object) {
        if (! (object instanceof Map) ) {
            throw new BadConfigurationException(where, object, "required a Map<String,String>");
        }
        Map<?,?> wildMap = (Map<?,?>) object;
        for (Map.Entry<?,?> entry : wildMap.entrySet()) {
            if (! (entry.getKey() instanceof String) || ! (entry.getValue() instanceof String) ) {
                throw new BadConfigurationException(where, object, "required a Map<String,String>");
            }
        }
        return launderStringsMap(wildMap);
    }

    /**
     * This method is simply here to isolate the unchecked warning suppression.
     * @param incoming the map to cast
     * @return the incoming map, casted
     */
    @SuppressWarnings("unchecked")
    private static Map<String,String> launderStringsMap(Map<?,?> incoming) {
        return (Map<String,String>) incoming;
    }

    /**
     * This method is simply here to isolate the unchecked warning suppression.
     * @param incoming the list to cast
     * @return the incoming list, casted
     */
    @SuppressWarnings("unchecked")
    private static List<String> launderStringsList(List<?> incoming) {
        return (List<String>) incoming;
    }

    // internal state

    final String sourceName;
    final Reader source;

    // nested classes

    private enum Command {
        BIND,
        BIND_AND_LOCK,
        LOCK,
        REQUIRE,
        REQUIRE_LOCKED,
        LOCKED,
        BOUND,
        ;
    }

    private static class BadConfigurationException extends RuntimeException {

        BadConfigurationException(String where, Object object) {
            this(where, object, "unknown error");
        }

        BadConfigurationException(String where, Object object, String message) {
            super(message);
            this.where = where;
            this.object = object;
            this.message = message;
        }

        String getWhere() {
            return where;
        }

        Object getObject() {
            return object;
        }

        private final String where;
        private final Object object;
        private final String message;
    }
}
