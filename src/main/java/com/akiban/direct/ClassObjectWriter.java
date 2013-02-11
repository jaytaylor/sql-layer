package com.akiban.direct;

import java.util.Stack;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;

public class ClassObjectWriter extends ClassBuilder {

    final ClassPool classPool;
    final String schema;
    final String packageName;
    Stack<CtClass> ctClasses = new Stack<CtClass>();
    CtClass currentCtClass;

    ClassObjectWriter(ClassPool classPool, String packageName, String schema) {
        this.classPool = classPool;
        this.packageName = packageName;
        this.schema = schema;
    }

    @Override
    public void preamble(String[] imports) {
        // Nothing to do. All names are fully qualified.
    }

    @Override
    public void startClass(String name) {
        currentCtClass = classPool.makeClass(name);
        ctClasses.push(currentCtClass);
    }

    @Override
    public void addMethod(String name, String returnType, String[] argumentTypes, String[] argumentNames, String[] body) {
        CtClass declaring = getCtClass(name);
        CtClass returnClass = getCtClass(returnType);
        CtClass[] parameters = new CtClass[argumentTypes.length];
        for (int i = 0; i < argumentTypes.length; i++) {
            parameters[i] = getCtClass(argumentTypes[i]);
        }
        CtMethod method = new CtMethod(returnClass, name, parameters, declaring);
        if (body != null) {
            StringBuilder sb = new StringBuilder("{");
            for (final String s : body) {
                sb.append(s);
            }
            sb.append("}");
            
            try {
                method.insertAfter(sb.toString());
            } catch (CannotCompileException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void end() {
        currentCtClass.freeze();
    }

    private CtClass getCtClass(final String name) {
        if ("void".equals(name)) {
            return CtClass.voidType;
        }
        if ("boolean".equals(name)) {
            return CtClass.booleanType;
        }
        if ("byte".equals(name)) {
            return CtClass.byteType;
        }
        if ("char".equals(name)) {
            return CtClass.charType;
        }
        if ("short".equals(name)) {
            return CtClass.shortType;
        }
        if ("int".equals(name)) {
            return CtClass.intType;
        }
        if ("float".equals(name)) {
            return CtClass.floatType;
        }
        if ("long".equals(name)) {
            return CtClass.longType;
        }
        if ("double".equals(name)) {
            return CtClass.doubleType;
        }
        CtClass c = classPool.getOrNull(fqn(name));
        if (c == null) {
            c = classPool.makeInterface(fqn(name));
        }
        return c;
    }

    private String fqn(final String name) {
        StringBuilder sb = new StringBuilder();
        sb.append(packageName);
        if (sb.length() > 0) {
            sb.append('.');
        }
        int depth = 0;
        for (final CtClass container : ctClasses) {
            sb.append(depth > 0 ? '$' : '.');
            sb.append(container.getSimpleName());
        }
        if (sb.length() > 0) {
            sb.append(depth > 0 ? '$' : '.');
        }
        sb.append(name);
        return sb.toString();
    }

}
