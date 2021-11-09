package engine.table.datatype.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.HashMap;

public class ClassLoaderObjectInputStream extends ObjectInputStream {
    protected final ClassLoader classLoader;

    public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader)
            throws IOException {
        super(in);
        this.classLoader = classLoader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException {
        if (classLoader != null) {
            String name = desc.getName();
            try {
                return Class.forName(name, false, classLoader);
            } catch (ClassNotFoundException ex) {
                // check if class is a primitive class
                Class<?> cl = primitiveClasses.get(name);
                if (cl != null) {
                    // return primitive class
                    return cl;
                } else {
                    // throw ClassNotFoundException
                    throw ex;
                }
            }
        }

        return super.resolveClass(desc);
    }

    @Override
    protected Class<?> resolveProxyClass(String[] interfaces)
            throws IOException, ClassNotFoundException {
        if (classLoader != null) {
            ClassLoader nonPublicLoader = null;
            boolean hasNonPublicInterface = false;

            // define proxy in class loader of non-public interface(s), if any
            Class<?>[] classObjs = new Class<?>[interfaces.length];
            for (int i = 0; i < interfaces.length; i++) {
                Class<?> cl = Class.forName(interfaces[i], false, classLoader);
                if ((cl.getModifiers() & Modifier.PUBLIC) == 0) {
                    if (hasNonPublicInterface) {
                        if (nonPublicLoader != cl.getClassLoader()) {
                            throw new IllegalAccessError(
                                    "conflicting non-public interface class loaders");
                        }
                    } else {
                        nonPublicLoader = cl.getClassLoader();
                        hasNonPublicInterface = true;
                    }
                }
                classObjs[i] = cl;
            }
            try {
                return Proxy.getProxyClass(
                        hasNonPublicInterface ? nonPublicLoader : classLoader, classObjs);
            } catch (IllegalArgumentException e) {
                throw new ClassNotFoundException(null, e);
            }
        }

        return super.resolveProxyClass(interfaces);
    }

    // ------------------------------------------------

    private static final HashMap<String, Class<?>> primitiveClasses = new HashMap<>(9);

    static {
        primitiveClasses.put("boolean", boolean.class);
        primitiveClasses.put("byte", byte.class);
        primitiveClasses.put("char", char.class);
        primitiveClasses.put("short", short.class);
        primitiveClasses.put("int", int.class);
        primitiveClasses.put("long", long.class);
        primitiveClasses.put("float", float.class);
        primitiveClasses.put("double", double.class);
        primitiveClasses.put("void", void.class);
    }
}
