package com.github.zorzr.test.flink;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class BaseFlinkTest {
    private static final AtomicReference<JobClient> jobReference = new AtomicReference<>();

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    public abstract void setupEnvironment(StreamExecutionEnvironment environment);

    public JobClient executeDefaultEnvironment() {
        try {
            StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
            setupEnvironment(environment);
            return environment.executeAsync();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void checkDefaultExecution() {
        if (jobReference.get() == null) {
            JobClient jobClient = executeDefaultEnvironment();
            jobReference.set(jobClient);
        }
    }


    public <T> T runTest(Supplier<T> evaluateTest) {
        return runTest(this::setupEnvironment, evaluateTest);
    }

    public <T> T runTest(Consumer<StreamExecutionEnvironment> setupEnv, Supplier<T> evaluateTest) {
        try {
            StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
            setupEnv.accept(environment);
            JobClient jobClient = environment.executeAsync();

            T output = evaluateTest.get();
            jobClient.cancel();
            return output;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
