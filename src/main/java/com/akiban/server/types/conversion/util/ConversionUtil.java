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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.akiban.server.types.conversion.util;

import com.akiban.server.types.AkType;
import org.joda.time.MutableDateTime;

public class ConversionUtil 
{
    private static final AbstractConverter<MutableDateTime> DATETIME = new DateTimeConverter();
    
    public static AbstractConverter getConverters (AkType type)
    {
        switch (type)
        {
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case YEAR:          return DATETIME;
            default: throw new UnsupportedOperationException ("Converters for " + type + " is not available yet");
        }
    }
}
