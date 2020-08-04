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
import com.streamsets.pipeline.api.Stage;

import javax.naming.InitialContext;

public class JmsMessageProcessorFactoryImpl implements JmsMessageProcessorFactory {

    @Override
    public JmsMessageProcessor create(
            InitialContext initialContext,
            JmsConnectionFactory connectionFactory,
            JmsProcessorConfig jmsProcessorConfig,
            Stage.Context context
    ) {
        return new JmsMessageProcessorImpl(
                initialContext,
                connectionFactory,
                jmsProcessorConfig,
                context
        );
    }
}
