package dev.bigspark.stage.processor.ibmmq;

import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import dev.bigspark.lib.ibmmq.config.InitialContextFactory;
import org.junit.Ignore;

public class JmsProcessorTest {


    @Ignore
    public void testWriteSingleRecord() throws Exception {

        JmsProcessorConfig jmsProcessorConfig = new JmsProcessorConfig();
        JmsMessageProcessorFactory jmsMessageProcessorFactory = new JmsMessageProcessorFactoryImpl();
        InitialContextFactory initialContextFactory = new InitialContextFactory();

        BaseStage stage = new JmsProcessor(jmsProcessorConfig, jmsMessageProcessorFactory, initialContextFactory);

        ProcessorRunner stageRunner = new ProcessorRunner.Builder(JmsProcessor.class, (Processor) stage).addOutputLane("output").build();

        stageRunner.runInit();
    }
}