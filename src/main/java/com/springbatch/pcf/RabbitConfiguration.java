package com.springbatch.pcf;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.springbatch.pcf.RabbitConstants.REPLY_QUEUE_NAME;
import static com.springbatch.pcf.RabbitConstants.REQUEST_QUEUE_NAME;

@Configuration
public class RabbitConfiguration {

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        return connectionFactory;
    }

    @Bean
    public Queue request()
    {
        return new Queue(REQUEST_QUEUE_NAME,false);
    }

    @Bean
    public Queue reply()
    {
        return new Queue(REPLY_QUEUE_NAME,false);
    }

    @Bean
    DirectExchange worksExchange() {
        return new DirectExchange("direct-exchange");
    }

    @Bean
    Binding requestBinder()
    {
        return BindingBuilder.bind(request()).to(worksExchange()).with(request().getName());
    }

    @Bean
    Binding replyBinder()
    {
        return BindingBuilder.bind(reply()).to(worksExchange()).with(reply().getName());
    }

    @Bean
    public RabbitTemplate worksRabbitTemplate() {
        RabbitTemplate r = new RabbitTemplate(connectionFactory());
        r.setExchange(worksExchange().getName());
        r.setRoutingKey(reply().getName());
        r.setConnectionFactory(connectionFactory());
        return r;
    }
}
