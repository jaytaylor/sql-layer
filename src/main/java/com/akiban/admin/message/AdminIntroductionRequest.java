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

package com.akiban.admin.message;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.akiban.message.MessageRequiredServices;
import com.akiban.server.service.session.Session;
import com.akiban.message.AkibanSendConnection;
import com.akiban.message.Request;

public class AdminIntroductionRequest extends Request
{
    // Request interface

    @Override
    public void read(ByteBuffer payload) throws Exception
    {
        super.read(payload);
        adminInitializer = readString(payload);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception
    {
        super.write(payload);
        writeString(payload, adminInitializer);
    }

    @Override
    public void execute(AkibanSendConnection connection, Session session, MessageRequiredServices requiredServices) throws Exception
    {
        // Executes in mysql head
    }

    // AdminIntroductionRequest interface

    public AdminIntroductionRequest(String adminInitializer) throws IOException
    {
        super(TYPE);
        // Have to send an absolute path to the other side
        this.adminInitializer = new File(adminInitializer).getCanonicalPath();
    }

    public AdminIntroductionRequest()
    {
        super(TYPE);
    }

    // State

    public static short TYPE;

    private String adminInitializer;
}
