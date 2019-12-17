package io.pivotal;

import benchmark.geode.data.Portfolio;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class PutAllApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        String statsFile = new File("home/vcap/logs/stats.gfs").getAbsolutePath();
        properties.setProperty("enable-time-statistics", "true");
        properties.setProperty("log-level", "config");
        properties.setProperty("statistic-sampling-enabled", "true");
        properties.setProperty("member-timeout", "8000");
        properties.setProperty("security-client-auth-init", "io.pivotal.ClientAuthInitialize.create");

        ClientCacheFactory ccf = new ClientCacheFactory(properties);
        ccf.setPdxSerializer(new ReflectionBasedAutoSerializer("benchmark.geode.data.*"));
        ccf.set("statistic-archive-file", statsFile);

        try{
            List<URI> locatorList = EnvParser.getInstance().getLocators();
            for (URI locator :
                    locatorList) {
                ccf.addPoolLocator(locator.getHost(), locator.getPort());
            }
            ClientCache clientCache = ccf.create();
            Region region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY)
                    .create("region");

            while (true) {
                HashMap tempMap = new HashMap<Long, Portfolio>();
                for (int i = 0; i < 1000; i++) {
                    long key = ThreadLocalRandom.current().nextLong(1, 1000000);
                    tempMap.put(key, new Portfolio(key));
                }
                region.putAll(tempMap);
            }

        }catch (Exception ex){
            throw new RuntimeException("Could not deploy Application", ex);
        }
    }
}
