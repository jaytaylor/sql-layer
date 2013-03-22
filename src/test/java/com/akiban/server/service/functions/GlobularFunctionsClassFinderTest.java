
package com.akiban.server.service.functions;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public final class GlobularFunctionsClassFinderTest {
    @Test
    public void findClasses() {
        FunctionsClassFinder finder = new GlobularFunctionsClassFinder("testfunctionpath.txt");
        Set<Class<?>> expected = new HashSet<>();
        expected.add(PathOneClass.class);
        expected.add(PathTwoClass.class);
        assertEquals(expected, finder.findClasses());
    }
}
