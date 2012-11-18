/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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

    public YamlConfiguration(String sourceName, Reader source, ClassLoader classLoader) {
        this.sourceName = sourceName;
        this.source = source;
        this.classLoader = classLoader;
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
            case PRIORITIZE:
                internalDoPrioritize( config, strings(where, commandValue) );
                break;
            default:
                throw new UnsupportedOperationException(command.name());
            }
        }
    }

    private void internalDoBind(ServiceConfigurationHandler config, Map<String,String> bindings) {
        for(Map.Entry<String,String> binding : bindings.entrySet()) {
            config.bind(binding.getKey(), binding.getValue(), classLoader);
        }
    }

    private void internalDoBindAndLock(ServiceConfigurationHandler config, Map<String,String> bindings) {
        for(Map.Entry<String,String> binding : bindings.entrySet()) {
            config.bind(binding.getKey(), binding.getValue(), classLoader);
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

    private void internalDoPrioritize(ServiceConfigurationHandler config, List<String> interfaceNames) {
        for (String interfaceName : interfaceNames) {
            config.prioritize(interfaceName);
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
    final ClassLoader classLoader;

    // nested classes

    private enum Command {
        BIND,
        BIND_AND_LOCK,
        LOCK,
        REQUIRE,
        REQUIRE_LOCKED,
        LOCKED,
        BOUND,
        PRIORITIZE,
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
