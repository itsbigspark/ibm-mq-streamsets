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

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import dev.bigspark.lib.ibmmq.config.InitialContextFactory;
import dev.bigspark.lib.ibmmq.config.JmsErrors;
import dev.bigspark.lib.ibmmq.config.JmsGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

public class JmsProcessor extends SingleLaneProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(JmsProcessor.class);
    private static final String JMS_TARGET_DATA_FORMAT_CONFIG_PREFIX = "dataFormatConfig.";
    private static final String JMS_TARGET_CONFIG_INITIAL_CTX_FACTORY = "jmsProcessorConfig.initialContextFactory";

    private final JmsProcessorConfig jmsProcessorConfig;
    private final JmsMessageProcessorFactory jmsMessageProcessorFactory;
    private final InitialContextFactory initialContextFactory;
    private JmsMessageProcessor jmsMessageProcessor;
    private JmsConnectionFactory connectionFactory;
    private int messagesSent;

    public JmsProcessor(
            JmsProcessorConfig jmsProcessorConfig,
            JmsMessageProcessorFactory jmsMessageProcessorFactory,
            InitialContextFactory initialContextFactory) {
        this.jmsProcessorConfig = jmsProcessorConfig;
        this.jmsMessageProcessorFactory = jmsMessageProcessorFactory;
        this.initialContextFactory = initialContextFactory;
        this.messagesSent = 0;
    }

    @Override
    public List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();
        InitialContext initialContext = null;

        try {
            Properties contextProperties = new Properties();
            contextProperties.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, jmsProcessorConfig.initialContextFactory);
            contextProperties.setProperty(javax.naming.Context.PROVIDER_URL, jmsProcessorConfig.providerURL);
            if (jmsProcessorConfig.initialContextFactory.toLowerCase(Locale.ENGLISH).contains("oracle")) {
                contextProperties.setProperty("db_url", jmsProcessorConfig.providerURL); // workaround for SDC-2068
            }
            contextProperties.putAll(jmsProcessorConfig.contextProperties);

            initialContext = initialContextFactory.create(contextProperties);
        } catch (NamingException ex) {
            LOG.info(
                    Utils.format(
                            JmsErrors.JMS_00.getMessage(),
                            jmsProcessorConfig.initialContextFactory,
                            jmsProcessorConfig.providerURL,
                            ex.toString()
                    ),
                    ex
            );
            issues.add(
                    getContext().createConfigIssue(
                            JmsGroups.JMS.name(),
                            JMS_TARGET_CONFIG_INITIAL_CTX_FACTORY,
                            JmsErrors.JMS_00,
                            jmsProcessorConfig.initialContextFactory,
                            jmsProcessorConfig.providerURL,
                            ex.toString()
                    )
            );
        }
        if (issues.isEmpty()) {
            try {
                if (jmsProcessorConfig.forceConnectionDetails) {
                    JmsFactoryFactory jmsFactoryFactory = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
                    connectionFactory = jmsFactoryFactory.createConnectionFactory();
                    connectionFactory.setStringProperty(WMQConstants.WMQ_HOST_NAME, jmsProcessorConfig.host);
                    connectionFactory.setIntProperty(WMQConstants.WMQ_PORT, jmsProcessorConfig.port);
                    connectionFactory.setStringProperty(WMQConstants.WMQ_CHANNEL, jmsProcessorConfig.channel);
                    connectionFactory.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
                    connectionFactory.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, jmsProcessorConfig.queueManager);
                } else {
                    connectionFactory = (JmsConnectionFactory) initialContext.lookup(jmsProcessorConfig.connectionFactory);
                }
            } catch (NamingException ex) {
                LOG.info(Utils.format(JmsErrors.JMS_01.getMessage(), jmsProcessorConfig.initialContextFactory, ex.toString()), ex);
                issues.add(getContext().createConfigIssue(JmsGroups.JMS.name(), JMS_TARGET_CONFIG_INITIAL_CTX_FACTORY, JmsErrors.JMS_01,
                        jmsProcessorConfig.connectionFactory, ex.toString()));
            } catch (JMSException ex) {
                LOG.info(Utils.format(JmsErrors.JMS_01.getMessage(), jmsProcessorConfig.host, ex.toString()), ex);
                issues.add(getContext().createConfigIssue(JmsGroups.JMS.name(), JMS_TARGET_CONFIG_INITIAL_CTX_FACTORY, JmsErrors.JMS_01,
                        jmsProcessorConfig.host, ex.toString()));
            }
        }
        if (issues.isEmpty()) {
            jmsMessageProcessor = jmsMessageProcessorFactory.create(
                    initialContext,
                    connectionFactory,
                    jmsProcessorConfig,
                    getContext()
            );
            issues.addAll(jmsMessageProcessor.init(getContext()));
        }

        return issues;
    }

    @Override
    public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
        messagesSent += this.jmsMessageProcessor.put(batch, singleLaneBatchMaker);
        jmsMessageProcessor.commit();
        LOG.debug("{}::{}", this.jmsProcessorConfig.destinationName, messagesSent);
    }

    @Override
    public void destroy() {
        if (this.jmsMessageProcessor != null) {
            this.jmsMessageProcessor.close();
        }
        super.destroy();
    }
}
