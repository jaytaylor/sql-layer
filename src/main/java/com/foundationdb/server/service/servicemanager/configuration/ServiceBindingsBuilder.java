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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class ServiceBindingsBuilder {

    // ServiceBindingsBuilder public interface

    public void bind(String interfaceName, String className, ClassLoader classLoader) {
        ServiceBinding binding = defineIfNecessary(interfaceName, classLoader);
        if (binding.isLocked()) {
            throw new ServiceConfigurationException(interfaceName + " is locked");
        }
        binding.setImplementingClass(className);
    }

    public Collection<ServiceBinding> getAllBindings(boolean strict) {
        markSectionEnd();
        Collection<ServiceBinding> all = new ArrayList<>(bindings.values());
        for (Iterator<ServiceBinding> iter = all.iterator(); iter.hasNext(); ) {
            ServiceBinding binding = iter.next();
            if (binding.isDirectlyRequired() && (binding.getImplementingClassName() == null)) {
                if (strict)
                    throw new ServiceConfigurationException(binding.getInterfaceName() + " is required but not bound");
                else
                    iter.remove();
            }
        }
        return all;
    }

    public List<String> getPriorities() {
        return priorities;
    }

    public void lock(String interfaceName) {
        require(interfaceName).lock();
    }

    public void markDirectlyRequired(String interfaceName) {
        defineIfNecessary(interfaceName, null).markDirectlyRequired();
    }

    public void mustBeBound(String interfaceName) {
        ServiceBinding binding = bindings.get(interfaceName);
        if ( (binding == null || binding.getImplementingClassName() == null)
                && !sectionRequirements.containsKey(interfaceName) )
        {
            sectionRequirements.put(interfaceName, false);
        }
    }

    public void mustBeLocked(String interfaceName) {
        ServiceBinding binding = bindings.get(interfaceName);
        if (binding == null || !binding.isLocked()) {
            sectionRequirements.put(interfaceName, true);
        }
    }

    public void prioritize(String interfaceName) {
        priorities.add(interfaceName);
    }

    public void markSectionEnd() {
        for (Map.Entry<String,Boolean> entry : sectionRequirements.entrySet()) {
            String interfaceName = entry.getKey();
            boolean lockRequired = entry.getValue();

            ServiceBinding binding = require(interfaceName);
            assert binding.getImplementingClassName() != null; // require makes this check
            if ( lockRequired && (!binding.isLocked()) ) {
                throw new ServiceConfigurationException(binding.getImplementingClassName() + " is not locked");
            }
        }
        sectionRequirements.clear();
    }

    public void unbind(String interfaceName) {
        ServiceBinding binding = bindings.remove(interfaceName);
        if (binding != null) {
            if (binding.isLocked()) {
                throw new ServiceConfigurationException("interface " + interfaceName +
                                                        " is locked and cannot be unbound");
            }
        }
    }

    // for testing

    void bind(String interfaceName, String className) {
        bind(interfaceName, className, null);
    }

    // private methods

    private ServiceBinding defineIfNecessary(String interfaceName, ClassLoader classLoader) {
        ServiceBinding binding = bindings.get(interfaceName);
        if (binding == null) {
            binding = new ServiceBinding(interfaceName);
            binding.setClassLoader(classLoader);
            bindings.put(interfaceName, binding);
        }
        else {
            if (binding.isLocked()) {
                if (binding.getClassLoader() != classLoader)
                    throw new ServiceConfigurationException("interface " + interfaceName +
                            " is locked, but bound to two ClassLoaders");
            }
            else {
                binding.setClassLoader(classLoader);
            }

        }
        return binding;
    }

    private ServiceBinding require(String interfaceName) {
        ServiceBinding binding = bindings.get(interfaceName);
        if (binding == null || binding.getImplementingClassName() == null) {
            throw new ServiceConfigurationException(interfaceName + " is not defined");
        }
        return binding;
    }

    // object state
    // invariant: key = bindings[key].getInterfaceName()
    private final Map<String, ServiceBinding> bindings = new HashMap<>();

    /**
     * This defines section requirements as a map of interface_name -> boolean. All interface names in this map
     * must be bound by the end of the current section; iff the entry's value is true, the binding must also be locked.
     * If we ever need to track more requirements, we should change the Boolean to an enum set.
     */
    private final Map<String,Boolean> sectionRequirements = new HashMap<>();

    /** Interfaces that ought to be run first (in order), as permitted
     * by dependencies. */
    private final List<String> priorities = new ArrayList<>();
}
