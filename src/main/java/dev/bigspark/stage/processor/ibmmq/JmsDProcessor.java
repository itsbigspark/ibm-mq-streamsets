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

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import dev.bigspark.lib.ibmmq.config.InitialContextFactory;

@StageDef(
    version = 1,
    label = "IBM MQ Processor",
    description = "Write data to IBM MQ and capture JMS header",
    icon = "processor.png",
    upgrader = JmsProcessorUpgrader.class,
    upgraderDef = "upgrader/JmsDProcessor.yaml",
    recordsByRef = true,
    onlineHelpRefUrl = "index.html?contextID=task_udk_yw5_n1b",
    services = @ServiceDependency(
        service = DataFormatGeneratorService.class,
        configuration = {
             @ServiceConfiguration(name = "displayFormats", value = "AVRO,BINARY,DELIMITED,JSON,PROTOBUF,SDC_JSON,TEXT,XML")
        }
    )
)
@ConfigGroups(JmsProcessorGroups.class)
@GenerateResourceBundle
public class JmsDProcessor extends DProcessor {

    @ConfigDefBean(groups = {"JMS"})
    public JmsProcessorConfig jmsProcessorConfig;

    @Override
    protected Processor createProcessor() {
        return new JmsProcessor(
                jmsProcessorConfig,
                new JmsMessageProcessorFactoryImpl(),
                new InitialContextFactory()
        );
    }
}
