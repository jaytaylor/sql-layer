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

import com.akiban.util.ArgumentValidation;

public final class ServiceBinding {

    // DefaultServiceBinding interface

    public boolean isLocked() {
        return locked;
    }

    public void lock() {
        locked = true;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        if (classLoader != null)
            this.classLoader = classLoader;
    }

    public String getInterfaceName() {
        return interfaceName;
    }

    public void setImplementingClass(String className) {
        if (isLocked()) {
            throw new ServiceConfigurationException("can't set new implementing class: " + interfaceName + " is locked");
        }
        implementingClassName = className;
    }

    public String getImplementingClassName() {
        return implementingClassName;
    }

    public boolean isDirectlyRequired() {
        return directlyRequired;
    }

    public void markDirectlyRequired() {
        directlyRequired = true;
    }

    // Object interface

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ServiceBinding)) return false;

        ServiceBinding that = (ServiceBinding) o;

        return !(interfaceName != null ? !interfaceName.equals(that.interfaceName) : that.interfaceName != null);

    }

    @Override
    public final int hashCode() {
        return interfaceName != null ? interfaceName.hashCode() : 0;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ServiceBinding(");
        builder.append(getInterfaceName()).append(" -> ").append(getImplementingClassName());
        builder.append(
                isLocked() ? ", locked)" : ", unlocked)"
        );
        return builder.toString();
    }

    // private methods

    // DefaultLockableServiceBinding interface

    public ServiceBinding(String interfaceName) {
        ArgumentValidation.notNull("interface name", interfaceName);
        this.interfaceName = interfaceName;
    }

    // object state

    private final String interfaceName;
    private String implementingClassName;
    private boolean locked;
    private boolean directlyRequired;
    private ClassLoader classLoader = ServiceBinding.class.getClassLoader();
}
