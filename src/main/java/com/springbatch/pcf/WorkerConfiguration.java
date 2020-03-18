package com.springbatch.pcf;

import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

import static com.springbatch.pcf.RabbitConstants.REPLY_QUEUE_NAME;

/**
 * This configuration class is for the worker side of the remote chunking sample.
 * It uses the {@link RemoteChunkingWorkerBuilder} to configure an
 * {@link IntegrationFlow} in order to:
 * <ul>
 *     <li>receive requests from the master</li>
 *     <li>process chunks with the configured item processor and writer</li>
 *     <li>send replies to the master</li>
 * </ul>
 *
 * @author Mahmoud Ben Hassine
 */
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
public class WorkerConfiguration {

    @Autowired
    private RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder;

    @Autowired
    RabbitConfiguration rabbitConfiguration;

    @Bean
    public DirectChannel workerRequests() {
        return new DirectChannel();
    }

    @Bean
    public SimpleMessageListenerContainer workerListenerContainer() {
        SimpleMessageListenerContainer container =
                new SimpleMessageListenerContainer(rabbitConfiguration.connectionFactory());
        container.setQueueNames(rabbitConfiguration.request().getName());
        container.setConcurrentConsumers(2);
        container.setDefaultRequeueRejected(false);
//        container.setAdviceChain(new Advice[]{interceptor()});
        return container;
    }


    @Bean
    public IntegrationFlow workerInboundFlow() {
        return IntegrationFlows
                .from(Amqp.inboundAdapter(workerListenerContainer()))
                .log()
                .handle(chunkProcessorChunkHandler())
                .channel(workerReplies())
                .get();
    }

    /*
     * Configure outbound flow (replies going to the master)
     */
   @Bean
    public DirectChannel workerReplies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow workerOutboundFlow() {
        return IntegrationFlows
                .from(workerReplies())
                .handle(Amqp.outboundAdapter(rabbitConfiguration.worksRabbitTemplate()).routingKey(REPLY_QUEUE_NAME))
                .get();
    }

    @Bean
    @ServiceActivator(inputChannel = "workerRequests", outputChannel = "workerReplies", sendTimeout = "10000")
    public ChunkProcessorChunkHandler<Integer> chunkProcessorChunkHandler() {
        ChunkProcessorChunkHandler chunkProcessorChunkHandler = new ChunkProcessorChunkHandler();
        chunkProcessorChunkHandler.setChunkProcessor(new SimpleChunkProcessor(itemProcessor(),itemWriter()));
        return chunkProcessorChunkHandler;
    }

    @Bean
    public ItemProcessor<Integer, Integer> itemProcessor() {
        return item -> {
            System.out.println("processing item " + item);
            return item;
        };
    }

    @Bean
    public ItemWriter<Integer> itemWriter() {
        return items -> {
            for (Integer item : items) {
                System.out.println("writing item " + item);
            }
        };
    }

    @Bean
    public IntegrationFlow workerIntegrationFlow() {
        return this.remoteChunkingWorkerBuilder
                .itemProcessor(itemProcessor())
                .itemWriter(itemWriter())
                .inputChannel(workerRequests())
                .outputChannel(workerReplies())
                .build();
    }

}