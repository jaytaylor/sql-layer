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
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private final static int BUFFER_SIZE = 65536;
    private final static String[] DIRECT_INTERFACES = { DirectContext.class.getName(), DirectModule.class.getName(),
            DirectObject.class.getName(), DirectList.class.getName(), AbstractDirectObject.class.getName(),
            DirectResultSet.class.getName(), };

    private final static String INCLUDE_PREFIX = "com.akiban.direct.script";

    final ClassPool pool;

    int depth = 0;

    final Set<String> generated = new HashSet<String>();

    public DirectClassLoader(ClassLoader baseLoader) {
        super(new URL[0], baseLoader);
        this.pool = new ClassPool(true);
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
        int p = genericName.indexOf('<');
        String name = p == -1 ? genericName : genericName.substring(0, p);

        /*
         * Not a parallel ClassLoader until necessary
         */
        synchronized (this) {
            Class<?> cl = findLoadedClass(name);

            if (cl == null && name.startsWith(INCLUDE_PREFIX)) {
                try {
                    /*
                     * These classes are included in the server jar, but we want
                     * them defined in this ClassLoader, not the parent.
                     */
                    String resourceName = name.replace('.', '/').concat(".class");
                    InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName);
                    if (is != null) {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int offset = 0;
                        while (true) {
                            int len = is.read(buffer, offset, buffer.length - offset);
                            if (len == -1) {
                                cl = defineClass(name, buffer, 0, offset);
                                break;
                            }
                            offset += len;
                            if (offset >= buffer.length) {
                                byte[] temp = new byte[buffer.length * 2];
                                System.arraycopy(buffer, 0, temp, 0, buffer.length);
                                buffer = temp;
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new ClassNotFoundException(name, e);
                }
            }
            /*
             * Load some interfaces and classes selected carefully by name from
             * the server's ClassLoader. These represent the server's context
             * within the module.
             */
            if (cl == null) {
                for (final String special : DIRECT_INTERFACES) {
                    if (special.equals(name)) {
                        cl = getClass().getClassLoader().loadClass(name);
                        if (cl == null) {
                            throw new ClassNotFoundException(name);
                        }
                        break;
                    }
                }
            }
            if (cl == null) {
                cl = compileSpecialClass(name, resolve);
            }

            if (cl == null) {
                try {
                    cl = getParent().loadClass(name);
                } catch (ClassNotFoundException e) {
                    // ignore
                }
            }

            if (cl == null) {
                cl = findClass(name);
            }
            if (resolve) {
                resolveClass(cl);
            }
            return cl;
        }
    }

    private Class<?> compileSpecialClass(final String name, final boolean resolve) {
        if (generated.contains(name)) {
            depth++;
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
                    final Class<?> c = defineClass(name, bytes, 0, bytes.length);
                    if (c != null && !c.isInterface() && resolve) {
                        /*
                         * Resolve here within nested depth so that
                         */
                        resolveClass(c);
                    }
                    return c;
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                depth--;
            }
        }
        return null;
    }

    public Class<? extends DirectModule> loadModule(AkibanInformationSchema ais, String moduleName, List<String> urls)
            throws Exception {
        if (urls != null) {
            for (final String u : urls) {
                URL url = new URL(u);
                addURL(url);
            }
        }
        @SuppressWarnings("unchecked")
        Class<? extends DirectModule> clazz = (Class<? extends DirectModule>) this.loadClass(moduleName);
        return clazz;
    }

    @SuppressWarnings("unchecked")
    public void registerDirectObjectClasses(Map<Integer, CtClass> implClasses) throws Exception {
        try { // TODO
            for (final CtClass c : implClasses.values()) {
                generated.add(c.getName());
                byte[] b = c.toBytecode();
                Class<? extends DirectObject> cl = (Class<? extends DirectObject>) defineClass(c.getName(), b, 0,
                        b.length);
                resolveClass(cl);
                if (!cl.isInterface()) {
                    Class<?> iface = cl.getInterfaces()[0];
                    Direct.registerDirectObjectClass(iface, (Class<? extends DirectObject>) cl);
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public void close() throws IOException {
        super.close();
        Direct.unregisterDirectObjectClasses();
    }

}
