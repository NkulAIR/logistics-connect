package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HubServiceApp {

    public record Hub(String hubId, String province, String sortingCenter, Boolean active) {};
    private static final Map<String, Hub> HUBS = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7051);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Serves provinces and sorting centers (place-name source of truth).)
        // Add domain endpoints for hub-service here.
        app.get("/hubs", ctx -> ctx.json(HUBS.values()));

        app.get("/hubs{hubId}", ctx ->{
            String id = ctx.pathParam("hubId").toUpperCase();
            Hub hub = HUBS.get(id);
            if (hub == null) ctx.status(404).json(Map.of("error", "hub not found: " + id));
            else ctx.json(hub);
        });
    }
}
