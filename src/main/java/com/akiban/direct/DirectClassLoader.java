/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
package com.akiban.direct;

import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import javassist.ClassPool;
import javassist.CtClass;

import com.akiban.ais.model.AkibanInformationSchema;

/**
 * ClassLoader that delegates selectively. This loader is used to
 * 
 * @author peter
 * 
 */
public class DirectClassLoader extends URLClassLoader {

    final ClassLoader parent;
    final ClassLoader bootstrap;
    final ClassPool pool;

    private final static String[] DIRECT_INTERFACES = { "com.akiban.direct.DirectContext",
            "com.akiban.direct.DirectModule", "com.akiban.direct.DaoPrototype" };

    public DirectClassLoader(ClassLoader bootstrap, ClassLoader parent, ClassPool pool) {
        super(new URL[0], bootstrap);
        this.bootstrap = bootstrap;
        this.parent = parent;
        this.pool = pool;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return loadClass(name, false);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        /*
         * Not a parallel ClassLoader until necessary
         */
        synchronized (this) {
            Class<?> cl = findLoadedClass(name);
            if (cl == null && name.startsWith("java")) {
                cl = parent.loadClass(name);
            }
            if (cl == null) {
                for (final String special : DIRECT_INTERFACES) {
                    if (special.equals(name)) {
                        cl = parent.loadClass(name);
                        if (cl == null) {
                            throw new ClassNotFoundException(name);
                        }
                        break;
                    }
                }
            }
            if (cl == null) {
                cl = compileSpecialClass(name);
            }
            if (cl == null) {
                try {
                    cl = findClass(name);
                } catch (ClassNotFoundException e) {
                    // fall through
                }
            }
            if (cl == null) {
                throw new ClassNotFoundException(name);
            }
            if (resolve) {
                resolveClass(cl);
            }
            return cl;
        }
    }

    private Class<?> compileSpecialClass(final String name) {
        /*
         * First check whether this is a precompiled interface
         */
        try {
            CtClass ctClass = pool.getOrNull(name);
            if (ctClass != null) {
                byte[] bytes = ctClass.toBytecode();
                try {
                    FileOutputStream os = new FileOutputStream("/tmp/" + name + ".class");
                    os.write(bytes);
                    os.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return defineClass(name, bytes, 0, bytes.length);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return null;
    }

    public Class<? extends DirectModule> loadModule(AkibanInformationSchema ais, String moduleName, List<String> urls)
            throws Exception {
        for (final String u : urls) {
            URL url = new URL(u);
            addURL(url);
        }
        @SuppressWarnings("unchecked")
        Class<? extends DirectModule> clazz = (Class<? extends DirectModule>) this.loadClass(moduleName);
        return clazz;
    }
}
