import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/** Calls the compiled pinned Scala companions; contains no metric implementation. */
public final class Oracle {
    private static final String PREFIX = "io.github.semyonsinchenko.sparkss.expressions.";
    private static final Map<String, String> MODULES = Map.ofEntries(
        Map.entry("ss_jaccard", "token.Jaccard"),
        Map.entry("ss_cosine", "token.Cosine"),
        Map.entry("ss_sorensen_dice", "token.SorensenDice"),
        Map.entry("ss_overlap_coefficient", "token.OverlapCoefficient"),
        Map.entry("ss_braun_blanquet", "token.BraunBlanquet"),
        Map.entry("ss_monge_elkan", "token.MongeElkan"),
        Map.entry("ss_levenshtein", "matrix.Levenshtein"),
        Map.entry("ss_lcs_similarity", "matrix.LcsSimilarity"),
        Map.entry("ss_jaro", "matrix.Jaro"),
        Map.entry("ss_jaro_winkler", "matrix.JaroWinkler"),
        Map.entry("ss_needleman_wunsch", "matrix.NeedlemanWunsch"),
        Map.entry("ss_smith_waterman", "matrix.SmithWaterman"),
        Map.entry("ss_affine_gap", "matrix.AffineGap"),
        Map.entry("ss_soundex", "phonetic.Soundex"),
        Map.entry("ss_refined_soundex", "phonetic.RefinedSoundex"),
        Map.entry("ss_double_metaphone", "phonetic.DoubleMetaphone")
    );
    private static final Map<String, Object> OBJECTS = new HashMap<>();
    private static final Map<String, Method> METHODS = new HashMap<>();
    private static Class<?> utf8;
    private static Method fromString;

    private Oracle() {}

    private static String b64(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static void metadata(String key, String value) {
        System.out.println("#meta\t" + key + "\t" + b64(value));
    }

    private static void origin(String key, Class<?> type) {
        metadata(key, type.getProtectionDomain().getCodeSource().getLocation().toExternalForm());
    }

    private static void uppercase() {
        for (int unit = 0; unit <= Character.MAX_VALUE; unit++) {
            char upper = Character.toUpperCase((char) unit);
            if (upper >= 'A' && upper <= 'Z') {
                System.out.println("#upper\t" + unit + "\t" + upper);
            }
        }
    }

    private static Object invoke(String function, String left, String right, String options)
            throws ReflectiveOperationException {
        String module = MODULES.get(function);
        if (module == null) {
            throw new IllegalArgumentException("Unknown function " + function);
        }
        Class<?> type = Class.forName(PREFIX + module + "$");
        Object companion = OBJECTS.get(function);
        if (companion == null) {
            companion = type.getField("MODULE$").get(null);
            OBJECTS.put(function, companion);
            origin("module:" + function, type);
        }
        boolean unary = module.startsWith("phonetic.");
        String[] params = options.isEmpty() ? new String[0] : options.split(",", -1);
        Class<?>[] signature;
        Object[] arguments;
        Object leftUtf8 = fromString.invoke(null, left);
        Object rightUtf8 = unary ? null : fromString.invoke(null, right);
        if (unary) {
            if (params.length != 0) throw new IllegalArgumentException("Unary options");
            signature = new Class<?>[] {utf8};
            arguments = new Object[] {leftUtf8};
        } else if (params.length == 0) {
            signature = new Class<?>[] {utf8, utf8};
            arguments = new Object[] {leftUtf8, rightUtf8};
        } else if (module.startsWith("token.") && !function.equals("ss_monge_elkan")) {
            if (params.length != 1) throw new IllegalArgumentException("Token options");
            signature = new Class<?>[] {utf8, utf8, int.class};
            arguments = new Object[] {leftUtf8, rightUtf8, Integer.valueOf(params[0])};
        } else if (function.equals("ss_monge_elkan")) {
            if (params.length != 2) throw new IllegalArgumentException("Monge options");
            signature = new Class<?>[] {utf8, utf8, String.class, int.class};
            arguments = new Object[] {leftUtf8, rightUtf8, params[0], Integer.valueOf(params[1])};
        } else if (function.equals("ss_jaro_winkler")) {
            if (params.length != 2) throw new IllegalArgumentException("Jaro-Winkler options");
            signature = new Class<?>[] {utf8, utf8, double.class, int.class};
            arguments = new Object[] {leftUtf8, rightUtf8, Double.valueOf(params[0]), Integer.valueOf(params[1])};
        } else if (function.equals("ss_needleman_wunsch") || function.equals("ss_smith_waterman")
                || function.equals("ss_affine_gap")) {
            if (params.length != 3) throw new IllegalArgumentException("Alignment options");
            signature = new Class<?>[] {utf8, utf8, int.class, int.class, int.class};
            arguments = new Object[] {leftUtf8, rightUtf8, Integer.valueOf(params[0]),
                                      Integer.valueOf(params[1]), Integer.valueOf(params[2])};
        } else {
            throw new IllegalArgumentException("Unsupported configured function " + function);
        }
        String methodKey = function + ":" + params.length;
        Method method = METHODS.get(methodKey);
        if (method == null) {
            method = type.getMethod(unary ? "encode" : "similarity", signature);
            METHODS.put(methodKey, method);
        }
        return method.invoke(companion, arguments);
    }

    public static void main(String[] args) throws Exception {
        metadata("java.version", System.getProperty("java.version"));
        metadata("java.vendor", System.getProperty("java.vendor"));
        uppercase();
        if (args.length == 1 && args[0].equals("--upper-ascii-only")) return;
        if (args.length != 0) throw new IllegalArgumentException("Unknown oracle option");
        utf8 = Class.forName("org.apache.spark.unsafe.types.UTF8String");
        fromString = utf8.getMethod("fromString", String.class);
        origin("utf8", utf8);
        origin("scala", Class.forName("scala.Predef$"));
        origin("commons-codec", Class.forName("org.apache.commons.codec.language.DoubleMetaphone"));
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = input.readLine()) != null) {
            String[] fields = line.split("\t", -1);
            if (fields.length != 5) throw new IllegalArgumentException("Expected five TSV fields");
            try {
                Object result = invoke(fields[1], decode(fields[2]), decode(fields[3]), fields[4]);
                if (result instanceof Double score) {
                    System.out.println(fields[0] + "\tscore\t" + Double.toString(score));
                } else {
                    System.out.println(fields[0] + "\tstring\t" + b64(result.toString()));
                }
            } catch (InvocationTargetException error) {
                throw new IllegalStateException("Scala oracle case " + fields[0] + " failed", error.getCause());
            }
        }
    }
}
