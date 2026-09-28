import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

/**
 * Disposable spike: dumps the public/protected API surface and the per-object fields of Flink's
 * stream-related classes as tab-separated rows, so the façade analysis is derived from the real
 * classes of each Flink line rather than from memory.
 */
public class Inventory {
    static final String[] CLASSES = {
        "org.apache.flink.streaming.api.datastream.DataStream",
        "org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator",
        "org.apache.flink.streaming.api.datastream.KeyedStream",
        "org.apache.flink.streaming.api.datastream.DataStreamSource",
        "org.apache.flink.streaming.api.datastream.SideOutputDataStream",
        "org.apache.flink.streaming.api.datastream.ConnectedStreams",
        "org.apache.flink.streaming.api.datastream.BroadcastConnectedStream",
        "org.apache.flink.streaming.api.datastream.BroadcastStream",
        "org.apache.flink.streaming.api.datastream.WindowedStream",
        "org.apache.flink.streaming.api.datastream.AllWindowedStream",
        "org.apache.flink.streaming.api.datastream.JoinedStreams",
        "org.apache.flink.streaming.api.datastream.CoGroupedStreams",
        "org.apache.flink.streaming.api.datastream.IterativeStream",
        "org.apache.flink.streaming.api.datastream.PartitionWindowedStream",
        "org.apache.flink.streaming.api.datastream.KeyedPartitionWindowedStream",
        "org.apache.flink.streaming.api.datastream.NonKeyedPartitionWindowedStream",
        "org.apache.flink.streaming.api.datastream.CachedDataStream",
        "org.apache.flink.streaming.api.datastream.DataStreamSink",
        "org.apache.flink.streaming.api.datastream.KeyedStream$IntervalJoin",
        "org.apache.flink.streaming.api.datastream.KeyedStream$IntervalJoined",
        "org.apache.flink.streaming.api.datastream.AsyncDataStream",
        "org.apache.flink.streaming.api.datastream.DataStreamUtils",
        "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment",
        "org.apache.flink.api.common.typeinfo.TypeInformation",
        "org.apache.flink.api.common.typeutils.TypeSerializer",
        "org.apache.flink.api.common.typeutils.TypeSerializerSnapshot",
        "org.apache.flink.api.common.functions.RuntimeContext",
        "org.apache.flink.api.common.state.StateDescriptor",
        "org.apache.flink.api.common.state.ValueStateDescriptor",
        "org.apache.flink.api.common.state.ListStateDescriptor",
        "org.apache.flink.api.common.state.MapStateDescriptor",
    };

    public static void main(String[] args) throws Exception {
        String line = args[0];
        for (String name : CLASSES) {
            Class<?> c;
            try {
                c = Class.forName(name, false, Inventory.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                row(line, "CLASS", name, "-", "absent", "", "", "", "");
                continue;
            }
            row(line, "CLASS", name, "-", mods(c.getModifiers()), ann(c), String.valueOf(c.getGenericSuperclass()), "", "");
            for (Constructor<?> k : c.getDeclaredConstructors()) {
                if (Modifier.isPrivate(k.getModifiers())) continue;
                row(line, "CTOR", name, "<init>", mods(k.getModifiers()) + (Modifier.isPublic(k.getModifiers()) || Modifier.isProtected(k.getModifiers()) ? "" : " package"), ann(k), "", params(k.getGenericParameterTypes()), "");
            }
            // Every public method a Kotlin caller can reach (includes inherited), plus protected ones a subclass can override.
            Map<String, Method> seen = new TreeMap<>();
            for (Class<?> k = c; k != null && k != Object.class; k = k.getSuperclass()) {
                for (Method m : k.getDeclaredMethods()) {
                    int mod = m.getModifiers();
                    if (m.isSynthetic() || m.isBridge()) continue;
                    if (!(Modifier.isPublic(mod) || Modifier.isProtected(mod))) continue;
                    String sig = m.getName() + Arrays.toString(m.getParameterTypes());
                    seen.putIfAbsent(sig, m);
                }
            }
            for (Method m : seen.values()) {
                row(line, "METHOD", name, m.getName(),
                    mods(m.getModifiers()) + (Modifier.isProtected(m.getModifiers()) ? " protected" : ""),
                    ann(m),
                    typeParams(m) + " " + m.getGenericReturnType().getTypeName(),
                    params(m.getGenericParameterTypes()),
                    m.getDeclaringClass().getSimpleName());
            }
            // Object-local state: instance fields declared on the class and its Flink superclasses.
            for (Class<?> k = c; k != null && k != Object.class; k = k.getSuperclass()) {
                for (Field f : k.getDeclaredFields()) {
                    if (Modifier.isStatic(f.getModifiers()) || f.isSynthetic()) continue;
                    row(line, "FIELD", name, f.getName(), vis(f.getModifiers()) + (Modifier.isFinal(f.getModifiers()) ? " final" : ""), "", f.getGenericType().getTypeName(), "", k.getSimpleName());
                }
            }
        }
    }

    static String typeParams(Method m) {
        TypeVariable<Method>[] tps = m.getTypeParameters();
        if (tps.length == 0) return "";
        StringJoiner j = new StringJoiner(",", "<", ">");
        for (TypeVariable<Method> t : tps) j.add(t.getName());
        return j.toString();
    }

    static String params(Type[] ps) {
        StringJoiner j = new StringJoiner(", ", "(", ")");
        for (Type t : ps) j.add(t.getTypeName());
        return j.toString();
    }

    static String vis(int mod) {
        if (Modifier.isPublic(mod)) return "public";
        if (Modifier.isProtected(mod)) return "protected";
        if (Modifier.isPrivate(mod)) return "private";
        return "package";
    }

    static String mods(int mod) {
        List<String> l = new ArrayList<>();
        if (Modifier.isFinal(mod)) l.add("final");
        if (Modifier.isStatic(mod)) l.add("static");
        if (Modifier.isAbstract(mod)) l.add("abstract");
        return String.join(" ", l);
    }

    static String ann(AnnotatedElement e) {
        List<String> l = new ArrayList<>();
        for (Annotation a : e.getAnnotations()) {
            String n = a.annotationType().getSimpleName();
            if (n.equals("Public") || n.equals("PublicEvolving") || n.equals("Internal") || n.equals("Experimental") || n.equals("Deprecated") || n.equals("VisibleForTesting")) l.add("@" + n);
        }
        return String.join(" ", l);
    }

    static void row(String... cols) {
        System.out.println(String.join("\t", cols));
    }
}
