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

package com.foundationdb.server.service.dxl;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;

class DXLMXBeanImpl implements DXLMXBean {
    private final DXLServiceImpl dxlService;
    private final SessionService sessionService;

    public DXLMXBeanImpl(DXLServiceImpl dxlService, SessionService sessionService) {
        this.dxlService = dxlService;
        this.sessionService = sessionService;
    }

    @Override
    public void dropAllGroups() {
        Session session = sessionService.createSession();
        try {
            for(TableName groupName : dxlService.ddlFunctions().getAIS(session).getGroups().keySet()) {
                dxlService.ddlFunctions().dropGroup(session, groupName);
            }
        } finally {
            session.close();
        }
    }

    @Override
    public String printAIS() {
        return new ProtobufWriter().save(ais()).toString();
    }

    private AkibanInformationSchema ais() {
        AkibanInformationSchema ais;
        Session session = sessionService.createSession();
        try {
            ais = dxlService.ddlFunctions().getAIS(session);
        } finally {
            session.close();
        }
        return ais;
    }
}
