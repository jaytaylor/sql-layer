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

package com.foundationdb.sql.server;

import com.foundationdb.ais.model.Routine;

import java.util.ArrayDeque;
import java.util.Deque;

public class ServerCallContextStack
{
    private final Deque<Entry> stack = new ArrayDeque<>();
    private final Deque<ServerSessionBase> callees = new ArrayDeque<>();
    private ServerTransaction sharedTransaction;
    private boolean firstCalleeNested;

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

    private static final ThreadLocal<ServerCallContextStack> tl = new ThreadLocal<ServerCallContextStack>() {
        @Override
        protected ServerCallContextStack initialValue() {
            return new ServerCallContextStack();
        }
    };

    public static ServerCallContextStack get() {
        return tl.get();
    }
    
    /** Convenience for use by Routines. */
    public static ServerQueryContext getCallingContext() {
        return get().current().getContext();
    }

    public Entry current() {
        return stack.peekLast();
    }
    
    public void push(ServerQueryContext context, ServerRoutineInvocation invocation) {
        stack.addLast(new Entry(context, invocation));
    }
    
    public void pop(ServerQueryContext context, ServerRoutineInvocation invocation,
                    boolean success) {
        Entry last = stack.removeLast();
        assert (last.getContext() == context);
        Thread.currentThread().setContextClassLoader(last.getContextClassLoader());
        if (stack.isEmpty()) {
            if (firstCalleeNested) {
                // Because ResultSets can be returned while still open, we
                // cannot close everything down until after the last call.
                while (!callees.isEmpty()) {
                    ServerSessionBase callee = callees.pop();
                    boolean active = callee.endCall(context, invocation, true, success);
                    assert !active : callee;
                }
                if (sharedTransaction != null) {
                    // We took over transaction from a nested call.
                    if (success) {
                        sharedTransaction.commit();
                    }
                    else {
                        sharedTransaction.rollback();
                    }
                    sharedTransaction = null;
                }
            }
            else {
                // This is the case where an embedded connection was made by
                // Java code, so there's no easy way to track its end-of-live.
                callees.clear();
                assert (sharedTransaction == null);
            }
        }
        else {
            while (true) {
                ServerSessionBase callee = callees.peek();
                if (callee == null) break;
                boolean active = callee.endCall(context, invocation, false, success);
                if (active) {
                    // If something is still open from this one, its transaction
                    // will need to be used by any subsequent ones, too.
                    if (sharedTransaction == null) {
                        sharedTransaction = callee.transaction;
                    }
                    break;
                }
                callees.pop();
            }
        }
    }

    public void addCallee(ServerSessionBase callee) {
        if (callees.isEmpty()) {
            firstCalleeNested = !stack.isEmpty();
        }
        callees.push(callee);
    }

    public ServerTransaction getSharedTransaction() {
        return sharedTransaction;
    }
}
