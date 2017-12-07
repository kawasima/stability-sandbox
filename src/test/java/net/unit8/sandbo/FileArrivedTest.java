package net.unit8.sandbo;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class FileArrivedTest {
    private static final Logger LOG = LoggerFactory.getLogger(FileArrivedTest.class);

    @Test
    public void test() throws IOException {
        RetryPolicy retryPolicy = new RetryPolicy()
                .withDelay(1, TimeUnit.SECONDS)
                .withMaxRetries(10)
                .retryOn(FileNotFoundException.class);
        try (InputStream in = Failsafe.with(retryPolicy)
                .onFailedAttempt((o, ex) -> LOG.warn("File is not found. Retry after 1minutes"))
                .get(() -> new FileInputStream("data.csv"))) {
            // ...
        }

    }
}
