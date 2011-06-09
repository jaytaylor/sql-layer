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

package com.akiban.server.service.servicemanager.configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ServiceBindingsBuilder {

    // ServiceBindingsBuilder public interface

    public void bind(String interfaceName, String className) {
        ServiceBinding binding = defineIfNecessary(interfaceName);
        if (binding.isLocked()) {
            throw new ServiceBindingException(interfaceName + " is locked");
        }
        binding.setImplementingClass(className);
    }

    public Collection<ServiceBinding> getAllBindings() {
        markSectionEnd();
        Collection<ServiceBinding> all = new ArrayList<ServiceBinding>(bindings.values());
        for (ServiceBinding binding : all) {
            if (binding.isDirectlyRequired() && (binding.getImplementingClassName() == null) ) {
                throw new ServiceBindingException(binding.getInterfaceName() + " is required but not bound");
            }
        }
        return all;
    }

    public void lock(String interfaceName) {
        require(interfaceName).lock();
    }

    public void markDirectlyRequired(String interfaceName) {
        defineIfNecessary(interfaceName).markDirectlyRequired();
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

    public void markSectionEnd() {
        for (Map.Entry<String,Boolean> entry : sectionRequirements.entrySet()) {
            String interfaceName = entry.getKey();
            boolean lockRequired = entry.getValue();

            ServiceBinding binding = require(interfaceName);
            assert binding.getImplementingClassName() != null; // require makes this check
            if ( lockRequired && (!binding.isLocked()) ) {
                throw new ServiceBindingException(binding.getImplementingClassName() + " is not locked");
            }
        }
        sectionRequirements.clear();
    }

    // private methods

    private ServiceBinding defineIfNecessary(String interfaceName) {
        ServiceBinding binding = bindings.get(interfaceName);
        if (binding == null) {
            binding = new DefaultServiceBinding(interfaceName);
            bindings.put(interfaceName, binding);
        }
        return binding;
    }

    private ServiceBinding require(String interfaceName) {
        ServiceBinding binding = bindings.get(interfaceName);
        if (binding == null || binding.getImplementingClassName() == null) {
            throw new ServiceBindingException(interfaceName + " is not defined");
        }
        return binding;
    }

    // object state
    // invariant: key = bindings[key].getInterfaceName()
    private final Map<String, ServiceBinding> bindings = new HashMap<String, ServiceBinding>();

    /**
     * This defines section requirements as a map of interface_name -> boolean. All interface names in this map
     * must be bound by the end of the current section; iff the entry's value is true, the binding must also be locked.
     * If we ever need to track more requirements, we should change the Boolean to an enum set.
     */
    private final Map<String,Boolean> sectionRequirements = new HashMap<String, Boolean>();
}
