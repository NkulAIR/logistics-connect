package co.wethinkcode.logisticsconnect;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import io.javalin.Javalin;

import static java.lang.Boolean.TRUE;
import static java.lang.Boolean.parseBoolean;

public class IngestionServiceApp {

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7050);

        app.get("/health", ctx -> ctx.result("OK"));
        // TODO: read and clean src/main/resources/hubs-global.csv (hubs, sorting centers, regional districts data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
        List<Hub> hubs = loadAndCleanHubs("/hubs-global.csv");
        app.get("/hubs", ctx -> ctx.json(hubs));


    }
    public static class Hub {
        String hubId;
        String province;
        String sortingCenter;
        Boolean active;

        @Override
        public String toString() {
            return "Hub{" + hubId + ", " + province + ", " + sortingCenter + ", active=" + active + "}";
        }
    }
    private static List<Hub> loadAndCleanHubs(String classpathResource) {
        List<Hub> hubs = new ArrayList<>();
        try (InputStream inputStream = IngestionServiceApp.class.getResourceAsStream(classpathResource)){
            if ( inputStream == null) throw new IOException("Resource not found");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                reader.readLine();
                String line;
                int rowNum = 1;

                while((line = reader.readLine()) != null){
                    rowNum ++;
                    if (line.isBlank()) continue;
                    String[] fields = line.split(",", -1);
                    if ( fields.length < 4 ) {
                        System.err.println("Skipping malformed row");
                        continue;
                    }
                    hubs.add(parseAndClean(fields, rowNum));
                }
            }

        } catch (IOException e){
            throw  new RuntimeException("Failed to load hubs CSV", e);
        }
        return hubs;
    }

    private static Hub parseAndClean(String[] fields, int rowNum) {
        Hub hub = new Hub();
        hub.hubId = trim_spaces(fields[0]).toUpperCase(Locale.ROOT);
        hub.province = normalizeProvince(trim_spaces(fields[1]));
        hub.sortingCenter = normalizeSortingCenter(trim_spaces(fields[2]));
        hub.active = parseBoolean(trim_spaces(fields[3]), rowNum);
        return hub;    }

    // Trims fields by collapsing double-spaces
    private static String trim_spaces(String field) {
        return field == null ? "" : field.strip().replaceAll("\\s+", " ");
    }

    private static final Map<String, String> PROVINCE_ALIASES = Map.ofEntries(
            Map.entry("gauteng", "Gauteng"),
            Map.entry("western cape", "Western Cape"),
            Map.entry("kwazulu-natal", "KwaZulu-Natal"),
            Map.entry("kwa-zulu natal", "KwaZulu-Natal"),
            Map.entry("kwazulu natal", "KwaZulu-Natal"),
            Map.entry("free state", "Free State"),
            Map.entry("eastern cape", "Eastern Cape"),
            Map.entry("limpopo", "Limpopo"),
            Map.entry("north west", "North West"),
            Map.entry("mpumalanga", "Mpumalanga"),
            Map.entry("northern cape", "Northern Cape")
    );

    static String toTitleCase(String s){
        if (s.isEmpty()) return s;
        StringBuilder sb = new StringBuilder();
        for (String w : s.toLowerCase(Locale.ROOT).split(" ")) {
            if (w.isEmpty()) continue;
            sb.append(Character.toUpperCase(w.charAt(0))).append(w.substring(1)).append(" ");
        }
        return sb.toString().strip();

    }


    static String normalizeProvince(String raw) {
        if (raw.isEmpty()) return null; // e.g. H-508 — flag as missing, don't guess
        String key = raw.toLowerCase(Locale.ROOT);
        String canonical = PROVINCE_ALIASES.get(key);
        if (canonical == null) {
            System.err.println("Unrecognized province spelling, keeping raw: " + raw);
            return toTitleCase(raw);
        }
        return canonical;    }


    private static final Set<String> TRUE_VALUES = Set.of("y", "yes", "true", "1");
    private static final Set<String> FALSE_VALUES = Set.of("n", "no", "false", "0");

    private static String normalizeSortingCenter(String raw) {
        return toTitleCase(raw);
    }
    private static Boolean parseBoolean(String raw, int rowNum) {
        String v = raw.toLowerCase(Locale.ROOT);
        if (TRUE_VALUES.contains(v)) return true;
        if (FALSE_VALUES.contains(v)) return false;
        System.err.println("Row " + rowNum + ": unresolved active value '" + raw + "', leaving null");
        // For unknown values
        return null;
    }

}
