
package com.akiban.sql.server;

import com.akiban.ais.model.Routine;

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
