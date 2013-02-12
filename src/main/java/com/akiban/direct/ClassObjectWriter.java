package com.akiban.direct;

import java.io.IOException;
import java.util.Stack;

import com.persistit.util.Util;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

public class ClassObjectWriter extends ClassBuilder {

    final ClassPool classPool;
    Stack<CtClass> ctClasses = new Stack<CtClass>();
    CtClass currentCtClass;

    public ClassObjectWriter(ClassPool classPool, String packageName, String schema) {
        super(packageName, schema);
        this.classPool = classPool;
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
        try {
            CtClass returnClass = getCtClass(returnType);
            CtClass[] parameters = new CtClass[argumentTypes.length];
            for (int i = 0; i < argumentTypes.length; i++) {
                parameters[i] = getCtClass(argumentTypes[i]);
            }
            CtMethod method = new CtMethod(returnClass, name, parameters, currentCtClass);
            if (body != null) {
                StringBuilder sb = new StringBuilder("{");
                for (final String s : body) {
                    sb.append(s);
                }
                sb.append("}");
                method.insertAfter(sb.toString());
            }
            currentCtClass.addMethod(method);
        } catch (CannotCompileException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void end() {
        try {
            byte[] b = currentCtClass.toBytecode();
            System.out.printf("\nClass %s\n%s\n", currentCtClass.getName(), Util.hexDump(b));
        } catch (IOException | CannotCompileException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        currentCtClass = ctClasses.pop();
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
        CtClass c = classPool.getOrNull(name);
        if (c == null) {
            c = classPool.makeInterface(name);
        }
        return c;
    }

}
