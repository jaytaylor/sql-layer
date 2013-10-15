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

package com.foundationdb.server;

import com.foundationdb.sql.Main;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class GetVersion
{
    public static void main(String[] args) throws Exception {
        if(args.length > 0 && "-v".equals(args[0])) {
            dumpVerbose();
        } else {
            dumpMinimal();
        }
    }

    private static void dumpVerbose() throws IllegalAccessException {
        for(Field field : Main.VERSION_INFO.getClass().getDeclaredFields()) {
            if((field.getModifiers() & Modifier.PUBLIC) > 0) {
                System.out.println(field.getName() + "=" + field.get(Main.VERSION_INFO));
            }
        }
    }

    private static void dumpMinimal() {
        System.out.println(Main.VERSION_INFO.versionLong);
    }
}
