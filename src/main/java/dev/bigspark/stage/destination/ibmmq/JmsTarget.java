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
package dev.bigspark.stage.destination.ibmmq;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
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

public class JmsTarget extends BaseTarget {
    private static final Logger LOG = LoggerFactory.getLogger(JmsTarget.class);
    private static final String JMS_TARGET_DATA_FORMAT_CONFIG_PREFIX = "dataFormatConfig.";
    private static final String JMS_TARGET_CONFIG_INITIAL_CTX_FACTORY = "jmsTargetConfig.initialContextFactory";

    private final JmsTargetConfig jmsTargetConfig;
    private final JmsMessageProducerFactory jmsMessageProducerFactory;
    private final InitialContextFactory initialContextFactory;
    private JmsMessageProducer jmsMessageProducer;
    private JmsConnectionFactory connectionFactory;
    private int messagesSent;

    public JmsTarget(
            JmsTargetConfig jmsTargetConfig,
            JmsMessageProducerFactory jmsMessageProducerFactory,
            InitialContextFactory initialContextFactory) {
        this.jmsTargetConfig = jmsTargetConfig;
        this.jmsMessageProducerFactory = jmsMessageProducerFactory;
        this.initialContextFactory = initialContextFactory;
        this.messagesSent = 0;
    }

    @Override
    public List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();
        InitialContext initialContext = null;

        try {
            Properties contextProperties = new Properties();
            contextProperties.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, jmsTargetConfig.initialContextFactory);
            contextProperties.setProperty(javax.naming.Context.PROVIDER_URL, jmsTargetConfig.providerURL);
            if (jmsTargetConfig.initialContextFactory.toLowerCase(Locale.ENGLISH).contains("oracle")) {
                contextProperties.setProperty("db_url", jmsTargetConfig.providerURL); // workaround for SDC-2068
            }
            contextProperties.putAll(jmsTargetConfig.contextProperties);

            initialContext = initialContextFactory.create(contextProperties);
        } catch (NamingException ex) {
            LOG.info(
                    Utils.format(
                            JmsErrors.JMS_00.getMessage(),
                            jmsTargetConfig.initialContextFactory,
                            jmsTargetConfig.providerURL,
                            ex.toString()
                    ),
                    ex
            );
            issues.add(
                    getContext().createConfigIssue(
                            JmsGroups.JMS.name(),
                            JMS_TARGET_CONFIG_INITIAL_CTX_FACTORY,
                            JmsErrors.JMS_00,
                            jmsTargetConfig.initialContextFactory,
                            jmsTargetConfig.providerURL,
                            ex.toString()
                    )
            );
        }
        if (issues.isEmpty()) {
            try {
                if (jmsTargetConfig.forceConnectionDetails) {
                    JmsFactoryFactory jmsFactoryFactory = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
                    connectionFactory = jmsFactoryFactory.createConnectionFactory();
                    connectionFactory.setStringProperty(WMQConstants.WMQ_HOST_NAME, jmsTargetConfig.host);
                    connectionFactory.setIntProperty(WMQConstants.WMQ_PORT, jmsTargetConfig.port);
                    connectionFactory.setStringProperty(WMQConstants.WMQ_CHANNEL, jmsTargetConfig.channel);
                    connectionFactory.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
                    connectionFactory.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, jmsTargetConfig.queueManager);
                } else {
                    connectionFactory = (JmsConnectionFactory) initialContext.lookup(jmsTargetConfig.connectionFactory);
                }
            } catch (NamingException ex) {
                LOG.info(Utils.format(JmsErrors.JMS_01.getMessage(), jmsTargetConfig.initialContextFactory, ex.toString()), ex);
                issues.add(getContext().createConfigIssue(JmsGroups.JMS.name(), JMS_TARGET_CONFIG_INITIAL_CTX_FACTORY, JmsErrors.JMS_01,
                        jmsTargetConfig.connectionFactory, ex.toString()));
            } catch (JMSException ex) {
                LOG.info(Utils.format(JmsErrors.JMS_01.getMessage(), jmsTargetConfig.host, ex.toString()), ex);
                issues.add(getContext().createConfigIssue(JmsGroups.JMS.name(), JMS_TARGET_CONFIG_INITIAL_CTX_FACTORY, JmsErrors.JMS_01,
                        jmsTargetConfig.host, ex.toString()));
            }
        }
        if (issues.isEmpty()) {
            jmsMessageProducer = jmsMessageProducerFactory.create(
                    initialContext,
                    connectionFactory,
                    jmsTargetConfig,
                    getContext()
            );
            issues.addAll(jmsMessageProducer.init(getContext()));
        }

        return issues;
    }

    @Override
    public void write(Batch batch) throws StageException {
        messagesSent += this.jmsMessageProducer.put(batch);
        jmsMessageProducer.commit();
        LOG.debug("{}::{}", this.jmsTargetConfig.destinationName, messagesSent);
    }

    @Override
    public void destroy() {
        if (this.jmsMessageProducer != null) {
            this.jmsMessageProducer.close();
        }
        super.destroy();
    }
}
