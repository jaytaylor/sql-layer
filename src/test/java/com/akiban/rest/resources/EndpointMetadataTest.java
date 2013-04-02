/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.rest.resources;

import static org.junit.Assert.*;

import org.junit.Test;

public class EndpointMetadataTest {

    private final static String[] VALID = {
            "method=GET path=somePath/([^/]*)/([^/]*) " + "function=functionName "
                    + "in=(QP:qp1 String required, QP:qp2 Date default '2010-02-03', "
                    + "PP:2 int required, PP:1 int default '123') " + "out=String",
            "method=POST path=raise function=raiseMySalary "
                    + "in=(json:empno int required, json:percent float required) out=void",
            "method=POST path=order/(\\\\d*)-(\\\\d+)/((\\\\d+)-\\\\d+)/confirm function=order "
                    + "in=(pp:1 int required, pp:2 int default '123', "
                    + "PP:3 String REQUIRED, CONTENT bytearray) out=void" };

    @Test
    public void createEndpointMetadata() throws Exception {

        for (final String spec : VALID) {
            String result = EndpointMetadata.createEndpointMetadata("someSchema", "someLibrary", spec).toString();
            assertEquals("Expected toString of a valid specification to match", spec.toLowerCase(), result.toLowerCase());
        }
    }

}
