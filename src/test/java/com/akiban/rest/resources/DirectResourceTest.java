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

import java.util.Arrays;

import org.junit.Test;

import com.akiban.rest.resources.DirectResource.EndpointMetadata;

public class DirectResourceTest {

    @Test
    public void createEndpointMetadata() throws Exception {

        final DirectResource resource = new DirectResource(null);
        EndpointMetadata md = resource.createEndpointMetadata(
                "abc",
                "/<ppa>/<ppb>",
                "jpa,jpb",
                "qp1,qp2",
                null,
                Arrays.asList(new String[] { "ppa int required", "ppb Date default '2013-01-02'", "jpb String required",
                        "jpa int", "qp2 int default 42 ", "qp1 float" }), "String default 'foo'");
        System.out.println(md);
    }

}
