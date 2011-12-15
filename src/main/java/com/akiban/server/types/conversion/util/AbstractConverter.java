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

import com.akiban.server.types.ValueSource;

public interface AbstractConverter<T>
{
    /**
     * 
     * @param source
     * @return a (java) object that could be created
     *  using source's value
     */
     T get (ValueSource source);
     
     /**
      * 
      * @param string
      * @return a (java) object that could be created
      *  using string's value
      */
     T get (String string);
}