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

    // TODO what about localhost or local ips

    @Test
    public void testParseThreeAllowedReferers() {
        List<URI> uris = CsrfProtectionFilter.parseAllowedReferers("http://my-site.com:45,https://other.site.com,http://wherever.edu");
        assertEquals(3, uris.size());
        assertUri("http", "my-site.com", 45, uris.get(0));
        assertUri("https", "other.site.com", -1, uris.get(1));
        assertUri("http", "wherever.edu", -1, uris.get(2));
    }
}
