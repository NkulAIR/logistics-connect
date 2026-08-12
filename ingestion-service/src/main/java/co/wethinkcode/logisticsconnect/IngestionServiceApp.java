package co.wethinkcode.logisticsconnect;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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

    private static String trim_spaces(String field) {
//      Trims fields by collapsing double-spaces
        return field == null ? "" : field.strip().replaceAll("\\s+", " ");
    }

    static String toTitleCase(String s){
        if (s.isEmpty()) return s;
        StringBuilder sb = new StringBuilder();
        for (String w : s.toLowerCase(Locale.ROOT).split(" ")) {
            if (w.isEmpty()) continue;
            sb.append(Character.toUpperCase(w.charAt(0))).append(w.substring(1)).append(" ");
        }
        return sb.toString().strip();

    }


    private static String normalizeSortingCenter(String trim) {
        return "";
    }

    private static String normalizeProvince(String trim) {
        return "";
    }
    private static Boolean parseBoolean(String trim, int rowNum) {
        return TRUE;
    }

}
