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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import dev.bigspark.lb.ibmmq.DefaultErrorRecordHandler;
import dev.bigspark.lb.ibmmq.ErrorRecordHandler;
import dev.bigspark.lb.ibmmq.RecordEL;
import dev.bigspark.lib.ibmmq.config.JmsErrors;
import dev.bigspark.lib.ibmmq.config.JmsGroups;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class JmsMessageProcessorImpl implements JmsMessageProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(JmsMessageProcessorImpl.class);
    private static final String CONN_FACTORY_CONFIG_NAME = "jmsProcessorConfig.connectionFactory";
    private final InitialContext initialContext;
    private final JmsConnectionFactory connectionFactory;
    private final JmsProcessorConfig jmsProcessorConfig;
    private final Stage.Context context;
    private final DataFormatGeneratorService generatorService;
    private ELEval destinationEval;
    private ErrorRecordHandler errorHandler;
    private Connection connection;
    private Session session;
    private Destination destination;
    private Destination replyToDestination;
    private LoadingCache<String, MessageProducer> messageProducers;

    public JmsMessageProcessorImpl(
            InitialContext initialContext,
            JmsConnectionFactory connectionFactory,
            JmsProcessorConfig jmsProcessorConfig,
            Stage.Context context
    ) {
        this.initialContext = initialContext;
        this.connectionFactory = connectionFactory;
        this.jmsProcessorConfig = jmsProcessorConfig;
        this.context = context;
        this.generatorService = context.getService(DataFormatGeneratorService.class);
    }

    @Override
    public List<Stage.ConfigIssue> init(Processor.Context context) {
        List<Stage.ConfigIssue> issues = new ArrayList<>();
        try {
            connectionFactory.setStringProperty(WMQConstants.JMS_IBM_MQMD_REPLYTOQMGR, "");
            connection = connectionFactory.createConnection();
        } catch (JMSException | StageException ex) {
            issues.add(context.createConfigIssue(JmsGroups.JMS.name(), CONN_FACTORY_CONFIG_NAME, JmsErrors.JMS_02,
                    connectionFactory.getClass().getName(), ex.toString()));
            LOG.info(Utils.format(JmsErrors.JMS_02.getMessage(), connectionFactory.getClass().getName(), ex.toString())
                    , ex);

        }
        if (issues.isEmpty()) {
            try {
                connection.start();
            } catch (JMSException ex) {
                issues.add(context.createConfigIssue(JmsGroups.JMS.name(), CONN_FACTORY_CONFIG_NAME, JmsErrors.JMS_04,
                        ex.toString()));
                LOG.info(Utils.format(JmsErrors.JMS_04.getMessage(), ex.toString()), ex);
            }
        }
        if (issues.isEmpty()) {
            try {
                session = connection.createSession(true, Session.SESSION_TRANSACTED);
            } catch (JMSException ex) {
                issues.add(context.createConfigIssue(JmsGroups.JMS.name(), CONN_FACTORY_CONFIG_NAME, JmsErrors.JMS_06,
                        ex.toString()));
                LOG.info(Utils.format(JmsErrors.JMS_06.getMessage(), ex.toString()), ex);
            }
        }
        if (issues.isEmpty() && StringUtils.isNotBlank(jmsProcessorConfig.replyToQueue)) {
            try {
                replyToDestination = session.createQueue(jmsProcessorConfig.replyToQueue);
            } catch (JMSException ex) {
                issues.add(context.createConfigIssue(JmsGroups.JMS.name(), CONN_FACTORY_CONFIG_NAME, JmsErrors.JMS_15,
                        jmsProcessorConfig.replyToQueue, ex.toString()));
                LOG.info(Utils.format(JmsErrors.JMS_15.getMessage(), jmsProcessorConfig.replyToQueue, ex.toString()), ex);
            }
        }
        if (issues.isEmpty()) {
            messageProducers = CacheBuilder.newBuilder()
                    .expireAfterAccess(15, TimeUnit.MINUTES)
                    .build(new CacheLoader<String, MessageProducer>() {
                        @Override
                        public MessageProducer load(String key) throws Exception {
                            switch (jmsProcessorConfig.destinationType) {
                                case UNKNOWN:
                                    destination = (Destination) initialContext.lookup(key);
                                    break;
                                case QUEUE:
                                    destination = session.createQueue(key);
                                    break;
                                case TOPIC:
                                    destination = session.createTopic(key);
                                    break;
                                default:
                                    throw new IllegalArgumentException(Utils.format("Unknown destination type: {}", jmsProcessorConfig.destinationName));
                            }

                            return session.createProducer(destination);
                        }
                    });

            destinationEval = context.createELEval("destinationName");
            errorHandler = new DefaultErrorRecordHandler(context);
        }

        return issues;
    }

    @Override
    public int put(Batch batch, SingleLaneProcessor.SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
        Iterator<Record> records = batch.getRecords();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        int count = 0;
        while (records.hasNext()) {
            baos.reset();
            Record record = records.next();

            try (DataGenerator generator = generatorService.getGenerator(baos)) {
                generator.write(record);
            } catch (IOException e) {
                LOG.error("Failed to write Records: {}", e);
                throw new StageException(JmsErrors.JMS_12, e.getMessage(), e);
            }

            handleDelivery(record, baos.toByteArray());
            singleLaneBatchMaker.addRecord(record);
            count++;
        }

        return count;
    }

    private void handleDelivery(Record record, byte[] payload) throws StageException {
        Message message;
        String destinationName = null;
        try {
            if (generatorService.isPlainTextCompatible()) {
                message = session.createTextMessage(new String(payload, generatorService.getCharset()));
            } else {
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(payload);
                message = bytesMessage;
            }
            //force a reply to destination if specified
            if (StringUtils.isNotBlank(jmsProcessorConfig.replyToQueue)) {
                message.setJMSReplyTo(replyToDestination);
            }

            // Resolve
            ELVars elVars = context.createELVars();
            RecordEL.setRecordInContext(elVars, record);
            destinationName = destinationEval.eval(elVars, jmsProcessorConfig.destinationName, String.class);

            // Finally sent the bits
            messageProducers.get(destinationName).send(message);
            record.getHeader().setAttribute("MessageId", message.getJMSMessageID());
        } catch (JMSException e) {
            LOG.error("Could not produce message: {}", e);
            throw new StageException(JmsErrors.JMS_13, e.getMessage(), e);
        } catch (UnsupportedEncodingException e) {
            LOG.error("Unsupported charset: {}", generatorService.getCharset());
            throw new StageException(JmsErrors.JMS_22, generatorService.getCharset(), e);
        } catch (ExecutionException e) {
            // The ExecutionException is a wrapper that guava cache will use to wrap any exception. We have handling for
            // some of the causes (like invalid destination name).
            if (e.getCause() instanceof NameNotFoundException) {
                errorHandler.onError(new OnRecordErrorException(
                        record, JmsErrors.JMS_05,
                        destinationName,
                        e.getCause().toString()
                ));
            } else {
                LOG.error("Can't create producer: " + e.toString(), e);
                throw new StageException(JmsErrors.JSM_14, e.toString(), e);
            }
        }
    }

    @Override
    public void commit() throws StageException {
        try {
            session.commit();
        } catch (JMSException ex) {
            throw new StageException(JmsErrors.JMS_08, ex.toString(), ex);
        }
    }

    @Override
    public void rollback() throws StageException {
        try {
            session.rollback();
        } catch (JMSException ex) {
            throw new StageException(JmsErrors.JMS_09, ex.toString(), ex);
        }
    }

    @Override
    public void close() {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException ex) {
                LOG.warn("Error closing session: " + ex, ex);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException ex) {
                LOG.warn("Error closing connection: " + ex, ex);
            }
        }
    }
}
