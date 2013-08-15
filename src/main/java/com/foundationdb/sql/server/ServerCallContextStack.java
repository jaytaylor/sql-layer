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

package com.foundationdb.sql.server;

import com.foundationdb.ais.model.Routine;

import java.util.ArrayDeque;
import java.util.Deque;

public class ServerCallContextStack
{
    private ServerCallContextStack() {
    }

    public static class Entry {
        private ServerQueryContext context;
        private ServerRoutineInvocation invocation;
        private ClassLoader contextClassLoader;

        private Entry(ServerQueryContext context, ServerRoutineInvocation invocation) {
            this.context = context;
            this.invocation = invocation;
            this.contextClassLoader = Thread.currentThread().getContextClassLoader();
        }

        public ServerQueryContext getContext() {
            return context;
        }
        
        public ClassLoader getContextClassLoader() {
            return contextClassLoader;
        }

        public ServerRoutineInvocation getInvocation() {
            return invocation;
        }

        public Routine getRoutine() {
            return invocation.getRoutine();
        }
    }

    private static final ThreadLocal<Deque<Entry>> tl = new ThreadLocal<Deque<Entry>>() {
        @Override
        protected Deque<Entry> initialValue() {
            return new ArrayDeque<>();
        }
    };

    public static Deque<Entry> stack() {
        return tl.get();
    }
    
    public static Entry current() {
        return stack().peekLast();
    }
    
    public static void push(ServerQueryContext context, ServerRoutineInvocation invocation) {
        stack().addLast(new Entry(context, invocation));
    }
    
    public static void pop(ServerQueryContext context, ServerRoutineInvocation invocation) {
        Entry last = stack().removeLast();
        assert (last.getContext() == context);
        Thread.currentThread().setContextClassLoader(last.getContextClassLoader());
    }
}
