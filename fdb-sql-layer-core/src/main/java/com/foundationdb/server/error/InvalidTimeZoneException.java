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

package com.foundationdb.server.error;

public class InvalidTimeZoneException extends StartupFailureException
{
    // you might expect this to take the invalid value, but, truth is we can't really know that what we have is what
    // was set. If you set the timezone to something invalid, and a Date() is created or TimeZone.getDefault() is called
    // the user.timezone will be set to something of the form: GMT-05:00
    // Note: on a mac that - (or +) will be replaced by an unprintable character, I filed a bug with openjdk.
    public InvalidTimeZoneException()
    {
        super(ErrorCode.INVALID_TIME_ZONE);
    }
}
