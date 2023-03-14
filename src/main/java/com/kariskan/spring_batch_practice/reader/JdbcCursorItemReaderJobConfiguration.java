package com.kariskan.spring_batch_practice.reader;

import com.kariskan.spring_batch_practice.domain.Pay;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

/**
 * CursorItemReader는 Paging과 다르게 Streaming으로 데이터를 처리한다. 즉, DB<->application 사이에 통로를 열고 하나씩 빨아들인다. JSP ResultSet.next()와
 * 비슷하다.
 */
@Slf4j
@RequiredArgsConstructor
@Configuration
public class JdbcCursorItemReaderJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource; // DataSource DI

    private static final int chunkSize = 10;

    @Bean
    public Job jdbcCursorItemReaderJob() {
        return jobBuilderFactory.get("jdbcCursorItemReaderJob")
                .start(jdbcCursorItemReaderStep())
                .build();
    }

    /**
     * reader는 Tasklet이 아니기 때문에 reader만으로는 수행될 수 없다. processor는 필수가 아니다.
     * reader에서 읽은 데이터에 대해 크게 변경 로직이 없다면 processor를 제외하고 writer만 구현해도된다.
     * <Pay, Pay>chunk(chunkSize): 첫번째 Pay는 Reader에서 반환할 타입이며, 두번째 Pay는 Writer에 파라미터로 넘어올 타입
     * chunkSize로 인자값을 넣은 경우는 Reader & Writer가 묶일 chunk 트랜잭션 범위이다.
     */
    @Bean
    public Step jdbcCursorItemReaderStep() {
        return stepBuilderFactory.get("jdbcCursorItemReaderStep")
                .<Pay, Pay>chunk(chunkSize)
                .reader(jdbcCursorItemReader())
                .writer(jdbcCursorItemWriter())
                .build();
    }

    /**
     * fetchSize: db에서 한번에 가져올 데이터 양을 나타낸다.
     * Paging과는 다른 것이, Paging은 실제 쿼리를 limit, offset을 이용해서 분할 처리하는 반면, Cursor는 분할 처리 없이 실행되나 내부적으로 가져오는 데이터는 fetchSize만큼 가져와 read()를 통해서 하나씩 가져온다.
     * dataSource: db에 접근하기 위해 사용할 Datasource 객체를 할당한다.
     * rowMapper: 쿼리 결과를 java instance로 매핑하기 위한 mapper이다. 보통 BeanPropertyRowMapper.class를 많이 사용한다.
     * sql: Reader로 사용할 쿼리문
     * name: reader의 이름을 지정한다. Bean의 이름이 아니며 Spring Batch의 ExecutionContext에서 저장되어질 이름이다.
     * 아래를 jdbcTemplate으로 구현하면 다음과 같다.
     * JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
     * List<Pay> payList = jdbcTemplate.query("SELECT id, amount, tx_name, tx_date_time FROM pay", new BeanPropertyRowMapper<>(Pay.class));
     */
    @Bean
    public JdbcCursorItemReader<Pay> jdbcCursorItemReader() {
        return new JdbcCursorItemReaderBuilder<Pay>()
                .verifyCursorPosition(false)
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Pay.class))
                .sql("SELECT id, amount, tx_name, tx_date_time FROM pay")
                .name("jdbcCursorItemReader")
                .build();
    }

    private ItemWriter<Pay> jdbcCursorItemWriter() {
        return list -> {
            for (Pay pay : list) {
                log.info("Current Pay={}", pay);
            }
        };
    }
}