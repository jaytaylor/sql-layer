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
package com.akiban.direct;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;

import com.akiban.ais.model.AkibanInformationSchema;

/**
 * ClassLoader that generates Akiban Direct classes. There is one instance of
 * this ClassLoader per schema per AkibanInformationSchema instance. The method
 * {@link AkibanInformationSchema#getDirectClassLoader(String, ClassLoader)}
 * should be used to acquire or create an instance of this class.
 * 
 * @author peter
 * 
 */
public class DirectClassLoader extends ClassLoader {

    final AkibanInformationSchema ais;
    final String schemaName;

    final ClassPool pool;
    final Set<String> generated = new HashSet<String>();

    Class<?> extentClass;
    boolean isGenerated;

    public DirectClassLoader(final ClassLoader parentLoader, final String schemaName, final AkibanInformationSchema ais) {
        super(parentLoader);
        this.pool = new ClassPool(true);
        this.schemaName = schemaName;
        this.ais = ais;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return loadClass(name, false);
    }

    /**
     * <p>
     * Implementation of {@link ClassLoader#loadClass(String, boolean)} to load
     * classes generated from the schema. These have been precompiled by
     * Javassist. As required to satisfy links in application code within the
     * module these are reduced to byte code and defined by the
     * {@link #ensureGenerated()} method.
     * </p>
     */
    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> cl = null;
        if (name.startsWith(ClassBuilder.PACKAGE)) {
            ensureGenerated();
            cl = findLoadedClass(name);
        }
        /*
         * Note: we must do the following because this is the ClassLoader
         * used by Rhino. See sun.org.mozilla.javascript.internal.NativeJavaTopPackage#init.
         */
        if (cl == null) {
            cl = getParent().loadClass(name);
        }
        if (resolve) {
            resolveClass(cl);
        }
        return cl;
    }

    synchronized void ensureGenerated() {
        if (!isGenerated) {
            try {
                /*
                 * Lazily generate Direct classes from schema
                 */
                if (ais != null && schemaName != null && schemaName.length() > 0) {
                    Map<Integer, CtClass> generatedClasses = ClassBuilder.compileGeneratedInterfacesAndClasses(ais,
                            schemaName);
                    this.registerDirectObjectClasses(generatedClasses);
                }
                isGenerated = true;
            } catch (Exception e) {
                throw new DirectException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void registerDirectObjectClasses(Map<Integer, CtClass> implClasses) throws IOException,
            CannotCompileException {
        try { // TODO
            for (final Entry<Integer, CtClass> entry : implClasses.entrySet()) {
                CtClass c = entry.getValue();
                generated.add(c.getName());
                byte[] b = c.toBytecode();
                Class<? extends DirectObject> cl = (Class<? extends DirectObject>) defineClass(c.getName(), b, 0,
                        b.length);
                resolveClass(cl);
                if (!cl.isInterface()) {
                    Class<?> iface = cl.getInterfaces()[0];
                    Direct.registerDirectObjectClass(iface, (Class<? extends AbstractDirectObject>) cl);
                }
                if (entry.getKey() == Integer.MAX_VALUE) {
                    extentClass = cl;
                }
            }
        } catch (IOException | CannotCompileException e) {
            throw e;
        }
    }

    public void close() throws IOException {
        Direct.unregisterDirectObjectClasses();
    }

    public Class<?> getExtentClass() {
        return extentClass;
    }

}
