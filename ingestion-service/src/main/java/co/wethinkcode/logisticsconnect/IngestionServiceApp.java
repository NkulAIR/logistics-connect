package co.wethinkcode.logisticsconnect;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import io.javalin.Javalin;


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

    private static final Set<String> PLACEHOLDERS = Set.of("", "n/a", "na", "unknown", "null", "-");

    private static String cleanField(String field) {
        String cleaned = trim_spaces(field);
        if (PLACEHOLDERS.contains(cleaned.toLowerCase(Locale.ROOT))) {
            return null; // Convert to proper null handling
        }
        return cleaned;
    }

    private static List<Hub> loadAndCleanHubs(String classpathResource) {
        Map<String, Hub> hubMap = new LinkedHashMap<>();
        Set<String> duplicateIds = new HashSet<>();
        Map<String, List<Integer>> duplicateOccurrences = new HashMap<>();
        int rowNum = 1;


        try (InputStream inputStream = IngestionServiceApp.class.getResourceAsStream(classpathResource)){
            if ( inputStream == null) throw new IOException("Resource not found");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

                String header = reader.readLine();
                if (header == null) {
                    throw new IOException("Empty CSV file");
                }


                String line;
                while((line = reader.readLine()) != null){
                    rowNum ++;
                    if (line.isBlank()) continue;

                    String[] fields = line.split(",", -1);
                    if (fields.length < 4) {
                        System.err.println("Row " + rowNum + ": malformed (expected 4 fields, got "
                                + fields.length + "), skipping");
                        continue;
                    }

                    String hubId       = cleanField(fields[0]);
                    String province    = cleanField(fields[1]);
                    String sortCenter  = cleanField(fields[2]);
                    String active      = cleanField(fields[3]);


                    if (hubId == null || province == null || sortCenter == null || active == null) {
                        System.err.println("Row " + rowNum + ": placeholder/missing value, skipping: " + line);
                        continue;
                    }

                    Hub hub = parseAndClean(hubId, province, sortCenter, active, rowNum);
                    if (hubMap.containsKey(hub.hubId)) {
                        duplicateIds.add(hub.hubId);
                        duplicateOccurrences
                                .computeIfAbsent(hub.hubId, k -> new ArrayList<>())
                                .add(rowNum);
                        System.err.println("Row " + rowNum + ": duplicate hubId '" + hub.hubId
                                + "' (first seen at row "
                                + duplicateOccurrences.get(hub.hubId).get(0) + "), skipping");
                        continue;
                    }
                    hubMap.put(hub.hubId, hub);

                }
                if (!duplicateIds.isEmpty()) {
                    System.err.println("\n=== DUPLICATE SUMMARY ===");
                    System.err.println("Total duplicate hub IDs found: " + duplicateIds.size());
                    for (String id : duplicateIds) {
                        System.err.println("  " + id + " appears in rows: " + duplicateOccurrences.get(id));
                    }
                    System.err.println("Keeping first occurrence only\n");
                }
                System.err.println("Successfully loaded " + hubMap.size()
                        + " unique hubs from " + (rowNum - 1) + " data rows");
            }

        } catch (IOException e){
            throw  new RuntimeException("Failed to load hubs CSV", e);
        }
        return new ArrayList<>(hubMap.values());
    }


    private static Hub parseAndClean(String hubId, String province,
                                     String sortingCenter, String active, int rowNum) {
        Hub hub = new Hub();
        hub.hubId = hubId.toUpperCase(Locale.ROOT);
        hub.province = normalizeProvince(province);
        hub.sortingCenter = normalizeSortingCenter(sortingCenter);
        hub.active = parseBoolean(active, rowNum);
        return hub;
    }

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
        if (raw == null || raw.isEmpty()) return null;

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
