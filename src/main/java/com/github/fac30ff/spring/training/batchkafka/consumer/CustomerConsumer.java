package com.github.fac30ff.spring.training.batchkafka.consumer;

import com.github.fac30ff.spring.training.batchkafka.entity.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Properties;

@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
@Log4j2
public class CustomerConsumer {
  public static void main(String[] args) {
    SpringApplication.run(CustomerConsumer.class, args);
  }
  private final StepBuilderFactory stepBuilderFactory;
  private final JobBuilderFactory jobBuilderFactory;
  private final KafkaProperties kafkaProperties;

  @Bean
  Job job() {
    return jobBuilderFactory.get("job")
            .incrementer(new RunIdIncrementer())
            .start(step())
            .build();
  }

  @Bean
  KafkaItemReader<Long, Customer> kafkaItemReader() {
    var props = new Properties();
    props.putAll(this.kafkaProperties.buildConsumerProperties());
    return new KafkaItemReaderBuilder<Long, Customer>()
            .partitions(1)
            .consumerProperties(props)
            .name("customers-reader")
            .saveState(true)
            .topic("customers")
            .build();
  }

  @Bean
  Step step() {
    var writer = new ItemWriter<Customer>() {
      @Override
      public void write(List<? extends Customer> list) throws Exception {
        list.forEach(item -> log.info("new customer: " + item));
      }
    };
    return stepBuilderFactory.get("step")
            .<Customer, Customer>chunk(10)
            .writer(writer)
            .reader(kafkaItemReader())
            .build();
  }
}
