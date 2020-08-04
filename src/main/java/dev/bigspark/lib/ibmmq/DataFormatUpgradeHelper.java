package dev.bigspark.lib.ibmmq;

import com.streamsets.pipeline.api.Config;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class DataFormatUpgradeHelper {
    private DataFormatUpgradeHelper() {
    }

    public static void upgradeAvroParserWithSchemaRegistrySupport(List<Config> configs) {
        List<Config> toRemove = new ArrayList();
        List<Config> toAdd = new ArrayList();
        Optional<Config> avroSchema = findByName(configs, "avroSchema");
        if (!avroSchema.isPresent()) {
            throw new IllegalStateException("Config 'avroSchema' is missing, this upgrader cannot be applied.");
        } else {
            String configName = ((Config)avroSchema.get()).getName();
            String prefix = configName.substring(0, configName.lastIndexOf("."));
            Optional<Config> schemaInMessage = findByName(configs, "schemaInMessage");
            if (schemaInMessage.isPresent()) {
                if (((Boolean)((Config)schemaInMessage.get()).getValue()).booleanValue()) {
                    toAdd.add(new Config(prefix + ".avroSchemaSource", "SOURCE"));
                } else {
                    toAdd.add(new Config(prefix + ".avroSchemaSource", "INLINE"));
                }

                toRemove.add((Config)schemaInMessage.get());
            } else {
                toAdd.add(new Config(prefix + ".avroSchemaSource", "SOURCE"));
            }

            toAdd.add(new Config(prefix + ".schemaRegistryUrls", new ArrayList()));
            toAdd.add(new Config(prefix + ".schemaLookupMode", "AUTO"));
            toAdd.add(new Config(prefix + ".subject", ""));
            toAdd.add(new Config(prefix + ".schemaId", Integer.valueOf(0)));
            configs.removeAll(toRemove);
            configs.addAll(toAdd);
        }
    }

    public static void upgradeAvroGeneratorWithSchemaRegistrySupport(List<Config> configs) {
        List<Config> toRemove = new ArrayList();
        List<Config> toAdd = new ArrayList();
        Optional<Config> avroSchema = findByName(configs, "avroSchema");
        if (!avroSchema.isPresent()) {
            throw new IllegalStateException("Config 'avroSchema' is missing, this upgrader cannot be applied.");
        } else {
            String configName = ((Config)avroSchema.get()).getName();
            String prefix = configName.substring(0, configName.lastIndexOf("."));
            Optional<Config> avroSchemaInHeader = findByName(configs, "avroSchemaInHeader");
            if (avroSchemaInHeader.isPresent()) {
                if (((Boolean)((Config)avroSchemaInHeader.get()).getValue()).booleanValue()) {
                    toAdd.add(new Config(prefix + ".avroSchemaSource", "HEADER"));
                } else {
                    toAdd.add(new Config(prefix + ".avroSchemaSource", "INLINE"));
                }

                toRemove.add((Config)avroSchemaInHeader.get());
            } else {
                toAdd.add(new Config(prefix + ".avroSchemaSource", "INLINE"));
            }

            toAdd.add(new Config(prefix + ".registerSchema", false));
            toAdd.add(new Config(prefix + ".schemaRegistryUrlsForRegistration", new ArrayList()));
            toAdd.add(new Config(prefix + ".schemaRegistryUrls", new ArrayList()));
            toAdd.add(new Config(prefix + ".schemaLookupMode", "AUTO"));
            toAdd.add(new Config(prefix + ".subject", ""));
            toAdd.add(new Config(prefix + ".subjectToRegister", ""));
            toAdd.add(new Config(prefix + ".schemaId", Integer.valueOf(0)));
            configs.removeAll(toRemove);
            configs.addAll(toAdd);
        }
    }

    public static void ensureAvroSchemaExists(List<Config> configs, String prefix) {
        Optional<Config> avroSchema = findByName(configs, "avroSchema");
        if (!avroSchema.isPresent()) {
            configs.add(new Config(prefix + ".avroSchema", (Object)null));
        }

    }

    static Optional<Config> findByName(List<Config> configs, String name) {
        Iterator var2 = configs.iterator();

        Config config;
        do {
            if (!var2.hasNext()) {
                return Optional.empty();
            }

            config = (Config)var2.next();
        } while(!config.getName().endsWith(name));

        return Optional.of(config);
    }
}

