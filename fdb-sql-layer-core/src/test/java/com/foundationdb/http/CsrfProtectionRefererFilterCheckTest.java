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

import com.foundationdb.junit.SelectedParameterizedRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SelectedParameterizedRunner.class)
public class CsrfProtectionRefererFilterCheckTest {

    private boolean isGetRequest;

    public CsrfProtectionRefererFilterCheckTest(boolean isGetRequest, String name) {
        this.isGetRequest = isGetRequest;
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> gatherCases() throws Exception {
            return Arrays.asList(new Object[]{true, "GET"}, new Object[]{false, "NOT GET"});
    }
    @Test
    public void testNullReferer() {
        // this might seem redundant considering all the tests below, but letting sites
        // with an empty referer through is a common mistake in referer checking, because
        // lots of browser or proxies will strip this info out, over privacy concerns.
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://somewhere.com");
        if (isGetRequest) {
            // GET requests are allowed to be blank or empty
            assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, null, isGetRequest));
        } else {
            assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, null, isGetRequest));
        }
    }

    @Test
    public void testEmptyReferer() {
        // this might seem redundant considering all the tests below, but letting sites
        // with an empty referer through is a common mistake in referer checking, because
        // lots of browser or proxies will strip this info out, over privacy concerns.
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://somewhere.com");
        if (isGetRequest) {
            // GET requests are allowed to be blank or empty
            assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "", isGetRequest));
        } else {
            assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "", isGetRequest));
        }
    }

    @Test
    public void testChecksScheme() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com:45");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:45", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://my-site.com:45", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "my-site.com:45", isGetRequest));
    }

    @Test
    public void testChecksScheme2() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("https://my-site.com:45");
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:45", isGetRequest));
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://my-site.com:45", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "my-site.com:45", isGetRequest));
    }

    @Test
    public void testChecksHost() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com:45");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:45", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://mysite.com:45", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site:45", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com.elsewhere.com:45", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://site.com:45", isGetRequest));
    }

    @Test
    public void testChecksPort() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com:45");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:45", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:450", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:145", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com", isGetRequest));
    }

    @Test
    public void testChecksDefaultPort() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com:80");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:80", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com", isGetRequest));
    }

    @Test
    public void testChecksDefaultPort2() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:80", isGetRequest));
    }

    @Test
    public void testCheckUuidUri() {
        String uuid = UUID.randomUUID().toString();
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("https://" + uuid + ".com");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://" + uuid + ".com", isGetRequest));
    }

    @Test
    public void testChecksIPGlobal() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("https://54.221.210.62");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://54.221.210.62", isGetRequest));
    }

    @Test
    public void testChecksIPV6GlobalWithPort() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4322");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4322", isGetRequest));
    }

    @Test
    public void testChecksIPV6VsIPV4() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("https://[::ffff:c000:0280]");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://[::ffff:c000:0280]", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://192.0.2.128", isGetRequest));
    }

    @Test
    public void testChecksIPV4VsIPV6() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("https://192.0.2.128");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://192.0.2.128", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://[::ffff:c000:0280]", isGetRequest));
    }

    @Test
    public void testChecksIPV4VsLocalhost() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("https://127.0.0.1");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://127.0.0.1", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://localhost", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://[::1]", isGetRequest));
    }

    @Test
    public void testChecksIPV6VsLocalhost() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("https://[::1]");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://[::1]", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://127.0.0.1", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://localhost", isGetRequest));
    }

    @Test
    public void testChecksIgnoresPath() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com/somewhere/specific", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://not-my-site.com/my-site.com", isGetRequest));
    }

    @Test
    public void testChecksIgnoresQuery() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com/?q=somewhere-specific", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://not-my-site.com?q=my-site.com", isGetRequest));
    }

    @Test
    public void testChecksIgnoresFragment() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com/#somewhere-specific", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://not-my-site.com/#my-site.com", isGetRequest));
    }


    @Test
    public void testChecksAgainstMultipleAllowed() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com,https://other-site.com:48");
        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://my-site.com", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://my-site.com:48", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://other-site.com", isGetRequest));

        assertTrue(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://other-site.com:48", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://other-site.com", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "https://my-site.com:48", isGetRequest));
        assertFalse(CsrfProtectionRefererFilter.isAllowedUri(uris, "http://other-site.com:48", isGetRequest));
    }
}
