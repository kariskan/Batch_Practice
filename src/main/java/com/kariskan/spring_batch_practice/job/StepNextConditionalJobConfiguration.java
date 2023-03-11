package com.kariskan.spring_batch_practice.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class StepNextConditionalJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    /**
     * step1 실패 시나리오: step1 -> step3
     * step1 성공 시나리오: step1 -> step2 -> step3
     *
     * on(): 캐치할 ExitStatus 지정, * 일 경우 모든 ExitStatus가 지정된다.
     *
     * to(): 다음으로 이동할 Step 지정
     *
     * from(): 일종의 이벤트 리스너 역할
     * 상태값을 보고 일치하는 상태라면 to()에 포함된 step을 호출한다.
     * step1의 이벤트 캐치가 FAILED로 돼있는 상태에서 추가로 이벤트 캐치하려면 from을 써야한다.
     *
     * end(): FlowBuilder를 반환하는 end와 FlowBuilder를 종료하는 end 2개가 있다.
     * on("*") 뒤에 있는 end는 FlowBuilder를 반환하는 end
     * build() 앞에 있는 end는 FlowBuilder를 종료하는 end
     * FlowBuilder를 반환하는 end 사용 시 계속해서 from을 이어갈 수 있다.
     */
    @Bean
    public Job stepNextConditionalJob() {
        return jobBuilderFactory.get("stepNextConditionalJob")
                .start(conditionalJobStep1())

                .on("FAILED") // FAILED 일 경우
                .to(conditionalJobStep3()) // step3으로 이동한다.
                .on("*") // step3의 결과 관계 없이
                .end() // step3으로 이동하면 Flow가 종료한다.

                .from(conditionalJobStep1()) // step1로부터
                .on("*") // FAILED 외에 모든 경우
                .to(conditionalJobStep2()) // step2로 이동한다.
                .next(conditionalJobStep3()) // step2가 정상 종료되면 step3으로 이동한다.
                .on("*") // step3의 결과 관계 없이
                .end() // step3으로 이동하면 Flow가 종료한다.
                .end() // Job 종료
                .build();
    }

    @Bean
    public Step conditionalJobStep1() {
        return stepBuilderFactory.get("step1")
                .tasklet((contribution, chunkContext) -> {
                    log.info(">>>>> This is stepNextConditionalJob Step1");

                    /**
                     ExitStatus를 FAILED로 지정한다.
                     해당 status를 보고 flow가 진행된다.
                     **/
                    contribution.setExitStatus(ExitStatus.FAILED);

                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Step conditionalJobStep2() {
        return stepBuilderFactory.get("conditionalJobStep2")
                .tasklet((contribution, chunkContext) -> {
                    log.info(">>>>> This is stepNextConditionalJob Step2");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Step conditionalJobStep3() {
        return stepBuilderFactory.get("conditionalJobStep3")
                .tasklet((contribution, chunkContext) -> {
                    log.info(">>>>> This is stepNextConditionalJob Step3");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }
}