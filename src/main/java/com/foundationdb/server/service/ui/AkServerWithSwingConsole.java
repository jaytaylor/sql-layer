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

package com.foundationdb.server.service.ui;

import com.foundationdb.server.AkServer;

import java.io.PrintStream;

public class AkServerWithSwingConsole
{
    public static void main(String[] args) {
        // This has to be done before log4j gets a chance to capture the previous
        // System.out for the CONSOLE appender. It will get switched to the real
        // console when that service starts up.
        PrintStream ps = new SwingConsole.TextAreaPrintStream();
        System.setOut(ps);
        try {
            AkServer.main(args);
        }
        catch (Exception ex) {
            ex.printStackTrace(ps);
        }
    }
}
