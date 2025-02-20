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
package dev.bigspark.stage.origin.ibmmq;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import dev.bigspark.lib.ibmmq.config.BasicConfig;
import dev.bigspark.lib.ibmmq.config.InitialContextFactory;
import dev.bigspark.lib.ibmmq.config.JmsGroups;
import dev.bigspark.lib.ibmmq.config.MessageConfig;

@StageDef(
        version = 7,
        label = "IBM MQ JMS Consumer",
        description = "Reads data from an IBM MQ JMS source.",
        icon = "origin.png",
        execution = ExecutionMode.STANDALONE,
        upgrader = JmsSourceUpgrader.class,
        upgraderDef = "upgrader/JmsDSource.yaml",
        recordsByRef = true,
        onlineHelpRefUrl = "index.html?contextID=task_zp1_4ck_dt",
        services = @ServiceDependency(
                service = DataFormatParserService.class,
                configuration = {
                        @ServiceConfiguration(name = "displayFormats", value = "AVRO,BINARY,DELIMITED,JSON,LOG,PROTOBUF,SDC_JSON,TEXT,XML")
                }
        )
)
@ConfigGroups(value = JmsGroups.class)
@GenerateResourceBundle
public class JmsDSource extends DSourceOffsetCommitter implements ErrorListener {

    @ConfigDefBean(groups = {"JMS"})
    public BasicConfig basicConfig;


    @ConfigDefBean(groups = {"JMS"})
    public MessageConfig messageConfig;

    @ConfigDefBean
    public JmsSourceConfig jmsConfig;

    @Override
    protected Source createSource() {
        return new JmsSource(basicConfig, jmsConfig,
                new JmsMessageConsumerFactoryImpl(), new JmsMessageConverterImpl(messageConfig),
                new InitialContextFactory());
    }

    @Override
    public void errorNotification(Throwable throwable) {
        JmsSource source = (JmsSource) getSource();
        source.rollback();
    }
}
