/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.http;

import org.junit.Test;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CsrfProtectionFilterTest {


    private void assertUri(String scheme, String host, int port, URI actualUri) {

        assertEquals("scheme", scheme, actualUri.getScheme());
        assertEquals("host", host, actualUri.getHost());
        assertEquals("port", port, actualUri.getPort());
    }


    @Test
    public void testParseNullAllowedReferers() {
        try {
            CsrfProtectionFilter.parseAllowedReferers(null);
            fail("ExpectedException");
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testParseEmptyAllowedReferers() {
        try {
            CsrfProtectionFilter.parseAllowedReferers("");
            fail("ExpectedException");
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testParseEmptyListOfAllowedReferers() {
        try {
            CsrfProtectionFilter.parseAllowedReferers(",,,");
            fail("ExpectedException");
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testParseOneAllowedReferers() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:45");
        assertEquals(1, uris.size());
        assertUri("http", "my-site.com", 45, uris.get(0));
    }

    // TODO what about localhost or local ips -- local ips could be a real threat, if you get onto a wifi or something
    // like that, perhaps we should pervent or warn or require an additional config
    // TODO what about null scheme or null port
    // TODO what about paths
    // TODO are there other parts of the URI?

    // TODO null schemes, hosts, ports, strings

    @Test
    public void testParseThreeAllowedReferers() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:45,https://other.site.com,http://wherever.edu");
        assertEquals(3, uris.size());
        assertUri("http", "my-site.com", 45, uris.get(0));
        assertUri("https", "other.site.com", -1, uris.get(1));
        assertUri("http", "wherever.edu", -1, uris.get(2));
    }

    @Test
    public void testParseAllowedReferersWithLeadingComma() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers(",https://other.site.com,http://wherever.edu");
        assertEquals(2, uris.size());
        assertUri("https", "other.site.com", -1, uris.get(0));
        assertUri("http", "wherever.edu", -1, uris.get(1));
    }

    @Test
    public void testParseBlankAllowedRefererInTheMiddle() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:45,,http://wherever.edu");
        assertEquals(2, uris.size());
        assertUri("http", "my-site.com", 45, uris.get(0));
        assertUri("http", "wherever.edu", -1, uris.get(1));
    }

    @Test
    public void testParseThreeAllowedReferersWithTrailingComma() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:45,https://other.site.com,http://wherever.edu,");
        assertEquals(3, uris.size());
        assertUri("http", "my-site.com", 45, uris.get(0));
        assertUri("https", "other.site.com", -1, uris.get(1));
        assertUri("http", "wherever.edu", -1, uris.get(2));
    }

    @Test
    public void testLooksLikeARegex() {
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my*site.com");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testLooksLikeARegex2() {
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my.*site.com");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testNoPathsAllowed() {
        // We don't check the paths on the referer, so don't let it be configured as such
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.subdomain.com/wherever?boo=3");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testBlankPathOk() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com/");
        assertEquals("uri.size()", 1, uris.size());
        assertUri("http","my-site.com",-1, uris.get(0));
    }

    @Test
    public void testNoUserAllowed() {
        // I don't know what they would be thinking
        // Also, according to the spec, the referer is not supposed to include user info or fragment components
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://user@my-site.com");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testNoAuthAllowed() {
        // I don't know what they would be thinking
        // Also, according to the spec, the referer is not supposed to include user info or fragment components
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://user:passw@my-site.com");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testAboutBlankProhibited() {
        // From the spec:
        //     If the target URI was obtained from a source that does not have its
        //     own URI (e.g., input from the user keyboard, or an entry within the
        //     user's bookmarks/favorites), the user agent MUST either exclude the
        //     Referer field or send it with a value of "about:blank".
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("about:blank");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testHostnameRequired() {
        // I don't know what they would be thinking
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("/boo");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testHostnameRequired2() {
        // I don't know what they would be thinking
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers(".");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testSchemeRequired() {
        // I don't know what they would be thinking
        try {
            List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("foundationdb.com");
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            /* passing */
        }
    }

    @Test
    public void testNullReferer() {
        // this might seem redundant considering all the tests below, but letting sites
        // with an empty referer through is a common mistake in referer checking, because
        // lots of browser or proxies will strip this info out, over privacy concerns.
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://somewhere.com");
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, null));
    }

    @Test
    public void testEmptyReferer() {
        // this might seem redundant considering all the tests below, but letting sites
        // with an empty referer through is a common mistake in referer checking, because
        // lots of browser or proxies will strip this info out, over privacy concerns.
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://somewhere.com");
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, ""));
    }

    @Test
    public void testChecksScheme() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:45");
        assertTrue(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com:45"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "https://my-site.com:45"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "my-site.com:45"));
    }

    @Test
    public void testChecksScheme2() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("https://my-site.com:45");
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com:45"));
        assertTrue(CsrfProtectionFilter.isAllowedUri(uris, "https://my-site.com:45"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "my-site.com:45"));
    }

    @Test
    public void testChecksHost() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:45");
        assertTrue(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com:45"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://mysite.com:45"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site:45"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com.elsewhere.com:45"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://site.com:45"));
    }

    @Test
    public void testChecksPort() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:45");
        assertTrue(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com:45"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com:450"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com:145"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com"));
    }

    @Test
    public void testChecksDefaultPort() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:80");
        assertTrue(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com:80"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com"));
    }

    @Test
    public void testChecksDefaultPort2() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com");
        assertTrue(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com"));
        assertFalse(CsrfProtectionFilter.isAllowedUri(uris, "http://my-site.com:80"));
    }

}
