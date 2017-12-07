package net.unit8.sandbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.jmx.JmxMeterRegistry;
import net.jodah.failsafe.CircuitBreaker;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.*;

public class ApiRequester {
    private static final Logger LOG = LoggerFactory.getLogger(ApiRequester.class);

    private OkHttpClient httpClient;
    private CircuitBreaker circuitBreaker;
    private RetryPolicy retryPolicy;
    private JmxMeterRegistry registry;
    private Counter counterRetry;
    private Counter counter503;

    public void initRegistry() {
        registry = new JmxMeterRegistry();
        counterRetry = registry.counter("retry");
        counter503 = registry.counter("503");
    }

    public ApiRequester() {
        retryPolicy = new RetryPolicy()
                .withMaxRetries(1)
                .withDelay(1, TimeUnit.SECONDS)
                .withJitter(.25)
                .retryOn(IOException.class)
                .retryIf(res -> Response.class.cast(res).code() >= 500);

        circuitBreaker = new CircuitBreaker()
                .withSuccessThreshold(3)
                .withFailureThreshold(5)
                .onClose(() ->    LOG.info("CircuitBreaker is closed"))
                .onHalfOpen(() -> LOG.info("CircuitBreaker is half-opened"))
                .onOpen(() ->     LOG.info("CircuitBreaker is opened"))
                .failOn(IOException.class)
                .failIf(res -> Response.class.cast(res).code() >= 500);
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(1, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();

        initRegistry();
    }

    public void requestAll() throws InterruptedException {
        BlockingQueue<Runnable> workerQueue = new ArrayBlockingQueue<>(512);
        ExecutorService service = new ThreadPoolExecutor(40,40,0, TimeUnit.NANOSECONDS, workerQueue,
                (r, executor) -> {
                    LOG.error("reject request");
                });

        for (int i = 0; i<1000; i++) {
            service.submit(() -> {
                Request request = new Request.Builder()
                        .url("http://localhost:3000/json")
                        .get()
                        .build();
                try {
                    Response response = Failsafe
                            .with(retryPolicy)
                            .with(circuitBreaker)
                            .withFallback(new Response.Builder()
                                    .protocol(Protocol.HTTP_1_1)
                                    .request(request)
                                    .code(503)
                                    .message("Circuit breaker is open")
                                    .build())
                            .onRetry((o, t) -> counterRetry.increment())
                            .get(() -> httpClient.newCall(request).execute());
                    if (response.code() == 200) {
                        try {
                            LOG.info(response.body().string());
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    } else {
                        counter503.increment();
                    }
                } catch (Throwable t) {
                    LOG.error("something wrong:", t);
                }
            });
            TimeUnit.MILLISECONDS.sleep(50);
        }
        service.shutdown();
        service.awaitTermination(100, TimeUnit.SECONDS);
        LOG.info("503={}", counter503.count());
        LOG.info("retry={}", counterRetry.count());
    }
    public static void main(String[] args) throws Exception {
        ApiRequester requester = new ApiRequester();
        requester.requestAll();
    }
}
