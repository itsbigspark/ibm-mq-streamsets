/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.bigspark.stage.processor.ibmmq;

import com.streamsets.pipeline.api.ConfigDef;
import dev.bigspark.lib.ibmmq.config.BaseJmsConfig;

public class JmsProcessorConfig extends BaseJmsConfig {

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        label = "JMS Destination Name",
        description = "Queue or topic name",
        displayPosition = 50,
        evaluation = ConfigDef.Evaluation.EXPLICIT,
        group = "JMS"
    )
    public String destinationName;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.BOOLEAN,
        defaultValue = "false",
        label = "Remove RFH2 Header",
        description = "Sets target client on queue to suppress the RFH2 header",
        displayPosition = 52,
        group = "JMS"
    )
    public Boolean removeRFH2Header = false;

    @ConfigDef(
        required = false,
        type = ConfigDef.Type.STRING,
        label = "JMS ReplyTo Queue",
        description = "ReplyTo Queue name",
        displayPosition = 55,
        evaluation = ConfigDef.Evaluation.EXPLICIT,
        group = "JMS"
    )
    public String replyToQueue;
}
