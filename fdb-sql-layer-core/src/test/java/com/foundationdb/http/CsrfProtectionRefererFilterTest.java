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
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CsrfProtectionRefererFilterTest {


    private void assertUri(String scheme, String host, int port, URI actualUri) {

        assertEquals("scheme", scheme, actualUri.getScheme());
        assertEquals("host", host, actualUri.getHost());
        assertEquals("port", port, actualUri.getPort());
    }

    private void assertSingleUri(String referer, String scheme, String host, int port) {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers(referer);
        assertEquals(1, uris.size());
        assertUri(scheme, host, port, uris.get(0));
    }

    private Exception assertIllegalAllowedReferers(String allowedReferers) {
        try {
            List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers(allowedReferers);
            fail("Expected an exception to be thrown; " + uris);
        } catch (IllegalArgumentException e) {
            return e;
        }
        throw new RuntimeException("it can't actually get here, but the compiler's not smart enough to figure that out.");
    }

    @Test
    public void testParseNullAllowedReferers() {
        assertIllegalAllowedReferers(null);
    }

    @Test
    public void testParseEmptyAllowedReferers() {
        assertIllegalAllowedReferers("");
    }

    @Test
    public void testParseEmptyListOfAllowedReferers() {
        assertIllegalAllowedReferers(",,,");
    }

    @Test
    public void testParseOneAllowedReferers() {
        assertSingleUri("http://my-site.com:45", "http", "my-site.com", 45);
    }

    @Test
    public void testParseThreeAllowedReferers() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com:45,https://other.site.com,http://wherever.edu");
        assertEquals(3, uris.size());
        assertUri("http", "my-site.com", 45, uris.get(0));
        assertUri("https", "other.site.com", -1, uris.get(1));
        assertUri("http", "wherever.edu", -1, uris.get(2));
    }

    @Test
    public void testParseAllowedReferersWithLeadingComma() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers(",https://other.site.com,http://wherever.edu");
        assertEquals(2, uris.size());
        assertUri("https", "other.site.com", -1, uris.get(0));
        assertUri("http", "wherever.edu", -1, uris.get(1));
    }

    @Test
    public void testParseBlankAllowedRefererInTheMiddle() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com:45,,http://wherever.edu");
        assertEquals(2, uris.size());
        assertUri("http", "my-site.com", 45, uris.get(0));
        assertUri("http", "wherever.edu", -1, uris.get(1));
    }

    @Test
    public void testParseThreeAllowedReferersWithTrailingComma() {
        List<URI> uris = CsrfProtectionRefererFilter.parseAllowedReferers("http://my-site.com:45,https://other.site.com,http://wherever.edu,");
        assertEquals(3, uris.size());
        assertUri("http", "my-site.com", 45, uris.get(0));
        assertUri("https", "other.site.com", -1, uris.get(1));
        assertUri("http", "wherever.edu", -1, uris.get(2));
    }

    @Test
    public void testLooksLikeARegex() {
        assertIllegalAllowedReferers("http://my*site.com");
    }

    @Test
    public void testErrorContainsBadURI() {
        Exception exception = assertIllegalAllowedReferers("http://site1.com,not-http.com,https://site2.com");
        assertThat(exception.getMessage(), containsString("not-http.com"));
    }

    @Test
    public void testLooksLikeARegex2() {
        assertIllegalAllowedReferers("http://my.*site.com");
    }

    @Test
    public void testNoPathsAllowed() {
        // We don't check the paths on the referer, so don't let it be configured as such
        assertIllegalAllowedReferers("http://my-site.subdomain.com/wherever?boo=3");
    }

    @Test
    public void testBlankPathOk() {
        assertSingleUri("http://my-site.com/", "http", "my-site.com", -1);
    }

    @Test
    public void testFragmentProhibited() {
        // We don't check the fragments on the referer, so don't let it be configured as such
        assertIllegalAllowedReferers("http://my-site.subdomain.com#earth");
    }

    @Test
    public void testNoUserAllowed() {
        // I don't know what they would be thinking
        // Also, according to the spec, the referer is not supposed to include user info or fragment components
        assertIllegalAllowedReferers("http://user@my-site.com");
    }

    @Test
    public void testNoAuthAllowed() {
        // I don't know what they would be thinking
        // Also, according to the spec, the referer is not supposed to include user info or fragment components
        assertIllegalAllowedReferers("http://user:passw@my-site.com");
    }

    @Test
    public void testAboutBlankProhibited() {
        // From the spec:
        //     If the target URI was obtained from a source that does not have its
        //     own URI (e.g., input from the user keyboard, or an entry within the
        //     user's bookmarks/favorites), the user agent MUST either exclude the
        //     Referer field or send it with a value of "about:blank".
        assertIllegalAllowedReferers("about:blank");
    }

    @Test
    public void testMustBeHttpOrHttpsFile() {
        assertIllegalAllowedReferers("file:///~/calendar");
    }

    @Test
    public void testMustBeHttpOrHttpsRandom() {
        assertIllegalAllowedReferers("mynewscheme://somewhere.com");
    }

    @Test
    public void testCanBeHttp() {
        assertSingleUri("http://my-site.com:45", "http", "my-site.com", 45);
    }

    @Test
    public void testCanBeHttps() {
        assertSingleUri("https://my-site.com:45", "https", "my-site.com", 45);
    }

    @Test
    public void testPortOptionalThere() {
        assertSingleUri("https://my-site.com:445", "https", "my-site.com", 445);
    }

    @Test
    public void testPortOptionalNotThere() {
        assertSingleUri("https://my-site.com", "https", "my-site.com", -1);
    }

    @Test
    public void testMustBeHierarchical() {
        assertIllegalAllowedReferers("http:my-site.com");
    }

    @Test
    public void testHostnameRequired() {
        // I don't know what they would be thinking
        assertIllegalAllowedReferers("/boo");
    }

    @Test
    public void testHostnameRequiredHttp() {
        // I don't know what they would be thinking
        assertIllegalAllowedReferers("http://");
    }

    @Test
    public void testHostnameRequiredHttps() {
        // I don't know what they would be thinking
        assertIllegalAllowedReferers("http://");
    }

    @Test
    public void testHostnameRequired2() {
        assertIllegalAllowedReferers(".");
    }

    @Test
    public void testHostnameRequired3() {
        assertIllegalAllowedReferers("boo");
    }

    @Test
    public void testSchemeRequired() {
        assertIllegalAllowedReferers("foundationdb.com");
    }

    @Test
    public void testEncodedCharacters() {
        // My understanding of the URI spec allows % encoded characters in the host name
        // but URI refuses to parse them. For now work under the assumption that host's
        // do not have weird characters.
        assertIllegalAllowedReferers("https://here%20is%20my%2Asweet%20uri.com");
    }

    @Test
    public void testUuidUri() {
        String uuid = UUID.randomUUID().toString();
        assertSingleUri("https://" + uuid + ".com", "https", uuid + ".com", -1);
    }

    @Test
    public void testLocalhost() {
        assertSingleUri("https://localhost", "https", "localhost", -1);
    }

    @Test
    public void testLocalhostWithPort() {
        assertSingleUri("http://localhost:4567", "http", "localhost", 4567);
    }

    @Test
    public void testIPLocalhost() {
        assertSingleUri("https://127.0.0.1", "https", "127.0.0.1", -1);
    }

    @Test
    public void testIPLocal192() {
        // RFC-1918: Private Address Space: 192.168.0.0     -   192.168.255.255 (192.168/16 prefix)
        assertSingleUri("http://192.168.1.100:9342", "http", "192.168.1.100", 9342);
    }

    @Test
    public void testIPLocal10() {
        // RFC-1918: Private Address Space: 10.0.0.0        -   10.255.255.255  (10/8 prefix)
        assertSingleUri("http://10.3.174.28:80", "http", "10.3.174.28", 80);
    }

    @Test
    public void testIPLocal172() {
        // RFC-1918: Private Address Space: 172.16.0.0      -   172.31.255.255  (172.16/12 prefix)
        assertSingleUri("http://172.16.38.254", "http", "172.16.38.254", -1);
    }

    @Test
    public void testIPGlobal() {
        assertSingleUri("https://54.221.210.62", "https", "54.221.210.62", -1);
    }

    @Test
    public void testIPV6Localhost() {
        assertSingleUri("https://[::1]", "https", "[::1]", -1);
    }

    @Test
    public void testIPV6Global() {
        assertSingleUri("https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]",
                "https", "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]", -1);
    }

    @Test
    public void testIPV6GlobalWithPort() {
        assertSingleUri("https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4322",
                "https", "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]", 4322);
    }


}
