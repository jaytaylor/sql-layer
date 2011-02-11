/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.api;
import com.akiban.ais.model.Index;
import com.akiban.server.service.session.Session;

import java.io.OutputStream;

public interface HapiProcessor {
    public void processRequest(Session session, HapiGetRequest request, HapiOutputter outputter, OutputStream outputStream)
            throws HapiRequestException;
    Index findHapiRequestIndex(Session session, HapiGetRequest request) throws HapiRequestException;
}
