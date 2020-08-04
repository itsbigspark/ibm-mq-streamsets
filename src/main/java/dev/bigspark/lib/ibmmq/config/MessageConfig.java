package dev.bigspark.lib.ibmmq.config;

import com.streamsets.pipeline.api.ConfigDef;

public class MessageConfig {
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "false",
            label = "Produce Single Record",
            description = "Generates a single record for multiple objects within a message",
            displayPosition = 3030,
            group = "#0"
    )
    public boolean produceSingleRecordPerMessage;

    public MessageConfig() {
    }
}
