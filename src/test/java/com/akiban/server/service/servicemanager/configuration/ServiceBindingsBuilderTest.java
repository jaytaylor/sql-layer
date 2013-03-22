
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
        List<ServiceBinding> sortedList = new ArrayList<>();
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
