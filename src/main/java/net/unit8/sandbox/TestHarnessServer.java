package net.unit8.sandbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class TestHarnessServer {
    private Undertow undertow;
    private KeyPairGenerator keyGen;
    private BlockingQueue taskQueue;
    private AtomicLong taskCounter = new AtomicLong(0);
    PrometheusMeterRegistry registry;
    Counter counter503;
    Timer responseTime;
    private static final AttachmentKey<Long> TASK_COUNTER_KEY = AttachmentKey.create(Long.class);
    private boolean enabledShedLoad = false;

    HttpHandler handler = exchange -> {
        if (exchange.getRequestPath().startsWith("/json")) {
            jsonResponse(exchange);
        } else if (exchange.getRequestPath().startsWith("/prometheus")) {
            String response = registry.scrape();
            exchange.setStatusCode(200);
            exchange.setResponseContentLength(response.length());
            exchange.getResponseSender().send(response);
        } else {
            exchange.setStatusCode(404);
        }
    };

    HttpHandler normalJsonReponseHandler = exchange -> {
        long t1 = System.currentTimeMillis();
        KeyPair key = keyGen.generateKeyPair();
        long elapse = System.currentTimeMillis() - t1;
        final Long taskNo = exchange.getAttachment(TASK_COUNTER_KEY);
        exchange.getResponseSender().send("{"
                + "\"id\":\"" + taskNo + ","
                + "\"key\":\"" + key.toString() + "\","
                + "\"elapse\":" + elapse
                + "}");
        responseTime.record(Duration.ofMillis(elapse));
        taskQueue.remove(taskNo);
    };

    public void initRegistry() {
        registry =new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        //new JvmMemoryMetrics().bindTo(registry);
        //new ProcessorMetrics().bindTo(registry);
        counter503 = registry.counter("503");
        responseTime = registry.timer("responseTime");
    }


    public void jsonResponse(HttpServerExchange exchange) {
        final long taskNo = taskCounter.getAndAdd(1);
        if (!enabledShedLoad || taskQueue.offer(taskNo)) {
            if (exchange.isInIoThread()) {
                exchange.putAttachment(TASK_COUNTER_KEY, taskNo);
                exchange.dispatch(normalJsonReponseHandler);
                return;
            }
        } else {
            counter503.increment();
            exchange.setStatusCode(503);
        }
    }

    public TestHarnessServer() throws NoSuchAlgorithmException {
        keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2500);

        initRegistry();

        undertow = Undertow.builder()
                .addHttpListener(3000, "0.0.0.0")
                .setHandler(handler)
                .setWorkerThreads(16)
                .build();
        taskQueue = new ArrayBlockingQueue(16);
        undertow.start();
    }


    public void shutdown() {
        undertow.stop();
    }

    public static void main(String[] args) throws Exception {
        final TestHarnessServer server = new TestHarnessServer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
        }));
    }
}
