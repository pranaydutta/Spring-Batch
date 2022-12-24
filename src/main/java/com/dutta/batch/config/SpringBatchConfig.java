package com.dutta.batch.config;

import com.dutta.batch.CustomerProcessor;
import com.dutta.batch.entity.Customer;
import com.dutta.batch.partition.ColumnRangePartitioner;
import com.dutta.batch.writer.CustomerWriter;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    //private CustomerRepository customerRepository;

    private CustomerWriter customerWriter;

    @Bean
    public FlatFileItemReader<Customer> reader()
    {
        FlatFileItemReader<Customer> itemReader=new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper=new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(true);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> beanMapper=new BeanWrapperFieldSetMapper<>();
        beanMapper.setTargetType(Customer.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(beanMapper);
        return lineMapper;
    }

    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }

    @Bean
    public ColumnRangePartitioner partitioner() {
        return new ColumnRangePartitioner();
    }


    @Bean
    public PartitionHandler partitionHandler()
    {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler =new TaskExecutorPartitionHandler ();
        taskExecutorPartitionHandler.setGridSize(4);
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(slaveStep());
        return taskExecutorPartitionHandler;
    }

    @Bean
    public Step slaveStep()
    {
        return stepBuilderFactory.get("slaveStep").<Customer, Customer>chunk(500)
                .reader(reader())
                .processor(processor())
                .writer(customerWriter)
                .faultTolerant()
               // .skipLimit(10000)
               // .skip(NumberFormatException.class)
                .listener(skipListner())
                .skipPolicy(skipPolicy())
                .build();
    }

    @Bean
    public SkipPolicy skipPolicy()
    {
        return new ExceptionSkipPolicy();
    }

    @Bean
    public SkipListener skipListner()
    {
        return new StepSkipListner();
    }

    @Bean
    public Step masterStep()
    {
        return stepBuilderFactory.get("masterStep")
                .partitioner(slaveStep().getName(),partitioner())
                .partitionHandler(partitionHandler())
                .build();
    }

/*
This writer bean use for normal batch processing,For chunk based ,we have created separate writer
 */
//    @Bean
//    public RepositoryItemWriter<Customer> writer()
//    {
//        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
//        writer.setRepository(customerRepository);
//        writer.setMethodName("save");
//        return writer;
//    }


    /*
    This bean use for normal batch processor
     */
//    @Bean
//    public Step step1() {
//        return stepBuilderFactory.get("csv-step").<Customer, Customer>chunk(10)
//                .reader(reader())
//                .processor(processor())
//                .writer(writer())
//                .taskExecutor(taskExecutor())
//                .build();
//    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("importCustomers")
                .flow(masterStep()).end().build();

    }
    /*
        This bean use for normal batch processing for execute task concurrently
     */
//    @Bean
//    public TaskExecutor taskExecutor() {
//        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
//        asyncTaskExecutor.setConcurrencyLimit(10);
//        return asyncTaskExecutor;
//    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);
        return taskExecutor;
    }
}
