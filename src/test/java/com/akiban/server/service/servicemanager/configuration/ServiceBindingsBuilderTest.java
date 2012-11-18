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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class ServiceBindingsBuilderTest {

    @Test
    public void bindOne() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.bind("one", "two");
        
        checkOnlyBinding(builder, "one", "two", false, false);
    }

    @Test
    public void mustBeBound_Good() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.mustBeBound("alpha");
        builder.bind("alpha", "puppy");
        builder.markSectionEnd();

        checkOnlyBinding(builder, "alpha", "puppy", false, false);
    }

    @Test(expected = ServiceConfigurationException.class)
    public void mustBeBound_ButIsNot() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.mustBeBound("alpha");
        builder.markSectionEnd();
    }

    @Test
    public void mustBeLocked_Good() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.mustBeLocked("alpha");
        builder.bind("alpha", "puppy");
        builder.lock("alpha");
        builder.markSectionEnd();

        checkOnlyBinding(builder, "alpha", "puppy", false, true);
    }

    @Test(expected = ServiceConfigurationException.class)
    public void mustBeLocked_Undefined() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.mustBeLocked("alpha");
        builder.markSectionEnd();
    }

    @Test(expected = ServiceConfigurationException.class)
    public void mustBeLocked_ButNotLocked() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.mustBeLocked("alpha");
        builder.bind("alpha", "beta");
        builder.markSectionEnd();
    }

    @Test(expected = ServiceConfigurationException.class)
    public void mustBeLocked_RequiredButNotLocked() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.bind("alpha", "beta");
        builder.mustBeLocked("alpha");
        builder.markDirectlyRequired("alpha");
        builder.markSectionEnd();
    }

    @Test(expected = ServiceConfigurationException.class)
    public void mustBeLocked_RequiredButNotLockedOrBound() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.mustBeLocked("alpha");
        builder.markDirectlyRequired("alpha");
        builder.markSectionEnd();
    }

    @Test
    public void markRequired_Good() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.markDirectlyRequired("alpha");
        builder.bind("alpha", "puppy");
        builder.markSectionEnd();

        checkOnlyBinding(builder, "alpha", "puppy", true, false);
    }

    @Test
    public void markRequired_ButIsNotBound() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.markDirectlyRequired("alpha");
        builder.markSectionEnd();
    }

    @Test
    public void lockTwice() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.bind("alpha", "puppy");
        builder.lock("alpha");
        builder.lock("alpha");
        builder.markSectionEnd();

        checkOnlyBinding(builder, "alpha", "puppy", false, true);
    }

    @Test
    public void lockThenRequireBound() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.bind("alpha", "puppy");
        builder.lock("alpha");
        builder.mustBeBound("alpha");

        checkOnlyBinding(builder, "alpha", "puppy", false, true);
    }

    @Test(expected = ServiceConfigurationException.class)
    public void lockUnbound() {
        new ServiceBindingsBuilder().lock("unbound.interface");
    }

    @Test(expected = ServiceConfigurationException.class)
    public void lockUnboundButRequired() {
        ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
        builder.markDirectlyRequired("hello");
        builder.lock("hello");
    }

    private static void checkBinding(String descriptor, ServiceBinding binding,
                                     String interfaceName, String implementingClass,
                                     boolean directlyRequired, boolean locked)
    {
        assertEquals(descriptor + ".interface", interfaceName, binding.getInterfaceName());
        assertEquals(descriptor + ".class", implementingClass, binding.getImplementingClassName());
        assertEquals(descriptor + ".required", directlyRequired, binding.isDirectlyRequired());
        assertEquals(descriptor + ".locked", locked, binding.isLocked());
    }

    private static void checkOnlyBinding(ServiceBindingsBuilder builder,
                                         String interfaceName, String implementingClass,
                                         boolean directlyRequired, boolean locked)
    {
        List<ServiceBinding> bindings = sorted(builder.getAllBindings(true));
        assertEquals("bindings count", 1, bindings.size());
        checkBinding("binding", bindings.get(0), interfaceName, implementingClass, directlyRequired, locked);
    }

    private static List<ServiceBinding> sorted(Collection<ServiceBinding> bindings) {
        List<ServiceBinding> sortedList = new ArrayList<ServiceBinding>();
        for (ServiceBinding binding : bindings) {
            sortedList.add(binding );
        }
        Collections.sort(sortedList, new Comparator<ServiceBinding>() {
            @Override
            public int compare(ServiceBinding o1, ServiceBinding o2) {
                return o1.getInterfaceName().compareTo(o2.getInterfaceName());
            }
        });
        return sortedList;
    }
}
