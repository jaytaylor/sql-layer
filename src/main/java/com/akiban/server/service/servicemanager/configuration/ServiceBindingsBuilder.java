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

package com.akiban.server.service.servicemanager.configuration;

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
        Collection<ServiceBinding> all = new ArrayList<ServiceBinding>(bindings.values());
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
    private final Map<String, ServiceBinding> bindings = new HashMap<String, ServiceBinding>();

    /**
     * This defines section requirements as a map of interface_name -> boolean. All interface names in this map
     * must be bound by the end of the current section; iff the entry's value is true, the binding must also be locked.
     * If we ever need to track more requirements, we should change the Boolean to an enum set.
     */
    private final Map<String,Boolean> sectionRequirements = new HashMap<String, Boolean>();

    /** Interfaces that ought to be run first (in order), as permitted
     * by dependencies. */
    private final List<String> priorities = new ArrayList<String>();
}
