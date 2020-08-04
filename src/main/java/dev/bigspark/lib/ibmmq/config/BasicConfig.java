package dev.bigspark.lib.ibmmq.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.DisplayMode;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.Stage.Context;
import dev.bigspark.lb.ibmmq.BasicErrors;

import java.util.List;

public class BasicConfig {
    @ConfigDef(
            required = true,
            type = Type.NUMBER,
            defaultValue = "1000",
            label = "Max Batch Size (records)",
            description = "Max number of records per batch",
            displayPosition = 1000,
            group = "#0",
            displayMode = DisplayMode.ADVANCED,
            min = 1L,
            max = 2147483647L
    )
    public int maxBatchSize = 1000;
    @ConfigDef(
            required = true,
            type = Type.NUMBER,
            defaultValue = "2000",
            label = "Batch Wait Time (ms)",
            description = "Max time to wait for data before sending a partial or empty batch",
            displayPosition = 1010,
            group = "#0",
            displayMode = DisplayMode.ADVANCED,
            min = 1L,
            max = 2147483647L
    )
    public int maxWaitTime = 2000;

    public BasicConfig() {
    }

    public void init(Context context, String group, String configPrefix, List<ConfigIssue> issues) {
        this.validate(context, group, configPrefix, issues);
    }

    private void validate(Context context, String group, String configPrefix, List<ConfigIssue> issues) {
        if (this.maxBatchSize < 1) {
            issues.add(context.createConfigIssue(group, configPrefix + "maxBatchSize", BasicErrors.BASIC_01, new Object[]{this.maxBatchSize}));
        }

        if (this.maxWaitTime < 1) {
            issues.add(context.createConfigIssue(group, configPrefix + "maxWaitTime", BasicErrors.BASIC_02, new Object[]{this.maxWaitTime}));
        }

    }
}