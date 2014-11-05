/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.service.restdml;

import static org.junit.Assert.*;

import org.junit.Test;

public class EndpointMetadataTest {

    private final static String[] VALID = {
            "method=GET path=somePath/([^/]*)/([^/]*) " + "function=functionName "
                    + "in=(QP:qp1 String required, QP:qp2 Date default '2010-02-03', "
                    + "PP:2 int required, PP:1 int default 123) " + "out=String",
            "method=POST path=raise function=raiseMySalary "
                    + "in=(json:empno int required, json:percent float required) out=void",
            "method=POST path=order/(\\\\d*)-(\\\\d+)/((\\\\d+)-\\\\d+)/confirm function=order "
                    + "in=(pp:1 int required, pp:2 int default '123', "
                    + "PP:3 String REQUIRED, CONTENT bytearray) out=void" };

    private final static String[] INVALID = {
            "method=FETCH path=somePath/([^/]*)/([^/]*) " + "function=functionName "
                    + "in=(QP:qp1 String required, QP:qp2 Date default '2010-02-03', "
                    + "PP:2 int required, PP:1 int default 123) " + "out=String",
            "method=POST path=raise function=raiseMySalary "
                    + "in=(json:123 int required, json:percent float required) out=void",
            "method=POST path=order/(\\\\d*)-(\\\\d+)/((\\\\d+)-\\\\d+)/confirm function=order "
                    + "in=(pp:1 int required, pp:2 int perhaps '123', "
                    + "PP:3 String REQUIRED, CONTENT bytearray) out=void" };

    @Test
    public void createEndpointMetadata() throws Exception {

        for (final String spec : VALID) {
            EndpointMetadata em1 = EndpointMetadata.createEndpointMetadata("someSchema", "someLibrary", spec);
            String canonical = em1.toString();
            EndpointMetadata em2 = EndpointMetadata.createEndpointMetadata("someSchema", "someLibrary", canonical);
            assertEquals("Parsed and re-parsed versions should be identical", em1, em2);
            assertEquals("Canonical string form is not stable", canonical, em2.toString());
        }

        for (final String spec : INVALID) {
            try {
                EndpointMetadata em1 = EndpointMetadata.createEndpointMetadata("someSchema", "someLibrary", spec);
                fail("Should not have been accepted: " + em1);
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }

}
