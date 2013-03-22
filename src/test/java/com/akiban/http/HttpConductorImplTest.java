
package com.akiban.http;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class HttpConductorImplTest {

    @Test
    public void standard() {
        check("/foo/*", "foo");
    }

    @Test
    public void unending() {
        check("/foo-bar", "foo-bar");
    }

    @Test(expected = IllegalPathRequest.class)
    public void badStart() {
        HttpConductorImpl.getContextPathPrefix("no-dash");
    }

    @Test(expected = IllegalPathRequest.class)
    public void empty() {
        HttpConductorImpl.getContextPathPrefix("");
    }

    @Test(expected = IllegalPathRequest.class)
    public void slashAll() {
        HttpConductorImpl.getContextPathPrefix("/*");
    }

    private void check(String full, String prefix) {
        assertEquals(full, prefix, HttpConductorImpl.getContextPathPrefix(full));
    }
}
