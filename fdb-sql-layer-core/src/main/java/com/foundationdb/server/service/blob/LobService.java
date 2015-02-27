/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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


package com.foundationdb.server.service.blob;

import com.foundationdb.server.service.session.Session;
import com.foundationdb.qp.operator.QueryContext;

import java.util.UUID;

public interface LobService {
    public void createNewLob(Session session, UUID lobId);
    public boolean existsLob(Session session, UUID lobId);
    public void deleteLob(Session session, UUID lobId);
    public void linkTableBlob(Session session, UUID lobId, int tableId);
    public long sizeBlob(Session session, UUID lobId);
    public byte[] readBlob(Session session, UUID lobId, long offset, int length);
    public byte[] readBlob(Session session, UUID lobId);
    public void writeBlob(Session session, UUID lobId, long offset, byte[] data);
    public void appendBlob(Session session, UUID lobId, byte[] data);
    public void truncateBlob(Session session, UUID lobId, long size);
    public void clearAllLobs(Session session);
    public void verifyAccessPermission(Session session, QueryContext context, UUID lobId);

}



