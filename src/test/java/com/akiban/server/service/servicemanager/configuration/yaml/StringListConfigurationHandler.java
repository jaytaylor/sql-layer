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

import com.akiban.server.service.servicemanager.configuration.ServiceConfigurationHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class StringListConfigurationHandler implements ServiceConfigurationHandler {

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

    // StringListStrategy interface

    public List<String> strings() {
        return unmodifiableStrings;
    }

    // private methods
    private void say(String format, Object... args) {
        strings.add(String.format(format, args));
    }

    // object state
    private final List<String> strings = new ArrayList<String>();
    private final List<String> unmodifiableStrings = Collections.unmodifiableList(strings);
}
