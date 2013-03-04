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
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

import com.akiban.ais.model.AkibanInformationSchema;

/**
 * ClassLoader that delegates selectively. This loader is used to
 * 
 * @author peter
 * 
 */
public class DirectClassLoader extends URLClassLoader {

    final AkibanInformationSchema ais;
    final String schemaName;
    final ClassPool pool;
    final Set<String> generated = new HashSet<String>();

    Class<?> extentClass;
    boolean isGenerated;

    public DirectClassLoader(final URL[] urls, final ClassLoader parentLoader, final String schemaName,
            final AkibanInformationSchema ais) {
        super(urls, parentLoader);
        this.pool = new ClassPool(true);
        this.schemaName = schemaName;
        this.ais = ais;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return loadClass(name, false);
    }

    /**
     * Implementation of {@link ClassLoader#loadClass(String, boolean)} to
     * handle special cases. For most classes, this implementation isolates
     * loaded classes from the core of Akiban Server so that loaded code cannot
     * access sensitive information within the server. (TODO: appropriate
     * security context to prevent file access, etc.)
     * 
     * Special cases:
     * <ul>
     * <li>Classes in the com.akiban.direct.script package such as
     * {@link com.akiban.direct.script.JSModule}. These are loaded within the
     * context of this class loader (not the parent) but are defined by byte
     * code found as a resource within the parent class loader. Translation: you
     * can write a module in this package as part of Akiban Server code but it
     * will be class-loaded in an isolated fashion. References to other Akiban
     * Server classes, except for a small white list, will not resolve at
     * runtime.</li>
     * <li>A white list of classes defined by {@link #DIRECT_INTERFACES} are
     * loaded from the parent. These include the DirectXXX interfaces and
     * AbstractDataObject which implements the core functionality of a
     * DirectObject.</li>
     * <li>Classes that are generated from the schema. These have been
     * precompiled by Javassist. As required to satisfy links in application
     * code within the module these are reduced to byte code and defined here.</li>
     * <li>All others are loaded from the base class loader provided as the
     * parent of this loader. This is intended to be the bootstrap classloader.</li>
     * </ul>
     * 
     * 
     */
    @Override
    protected Class<?> loadClass(String genericName, boolean resolve) throws ClassNotFoundException {
        // TODO - still needed? I think not.
        int p = genericName.indexOf('<');
        String name = p == -1 ? genericName : genericName.substring(0, p);

        Class<?> cl = null;
        try {
            cl = getParent().loadClass(name);
        } catch (ClassNotFoundException e) {
            // ignore
        }

        if (cl == null) {
            cl = findClass(name);
        }

        if (cl == null) {
            /*
             * Lazily generate Direct classes when needed.
             */
            try {
                generate();
            } catch (Exception e) {
                e.printStackTrace();
            }

            /*
             * Not a parallel ClassLoader until necessary
             */
            synchronized (this) {
                if (generated.contains(name)) {
                    try {
                        /*
                         * First check whether this is a precompiled interface
                         */
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
                            cl = defineClass(name, bytes, 0, bytes.length);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }

            }
        }
        if (resolve) {
            resolveClass(cl);
        }
        return cl;
    }

    @SuppressWarnings("unchecked")
    public void registerDirectObjectClasses(Map<Integer, CtClass> implClasses) throws Exception {
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
        } catch (Exception e) {
            throw e;
        }
    }

    private synchronized void generate() throws Exception {
        if (!isGenerated) {
            isGenerated = true;
            Map<Integer, CtClass> generated = ClassBuilder.compileGeneratedInterfacesAndClasses(ais, schemaName);
            this.registerDirectObjectClasses(generated);
        }
    }

    public void close() throws IOException {
        super.close();
        Direct.unregisterDirectObjectClasses();
    }

    public Class<?> getExtentClass() {
        return extentClass;
    }

}
