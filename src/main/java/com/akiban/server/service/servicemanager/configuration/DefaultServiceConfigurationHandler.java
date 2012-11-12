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

import java.util.Collection;
import java.util.List;

public final class DefaultServiceConfigurationHandler implements ServiceConfigurationHandler {

    // YamlConfigurationStrategy interface

    @Override
    public void bind(String interfaceName, String implementingClassName) {
        builder.bind(interfaceName, implementingClassName);
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

    // DefaultServiceConfigurationHandler interface

    public Collection<ServiceBinding> serviceBindings(boolean strict) {
        return builder.getAllBindings(strict);
    }

    public List<String> priorities() {
        return builder.getPriorities();
    }

    // object state
    private final ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
}
