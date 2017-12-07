package net.unit8.sandbo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SQLTest {
    private static final Logger LOG = LoggerFactory.getLogger(SQLTest.class);

    HikariPool hikariCP;
    RetryPolicy selectRetryPolicy = new RetryPolicy()
            .withBackoff(50, 3000, TimeUnit.MILLISECONDS)
            .withJitter(.25)
            .withMaxRetries(3)
            .abortIf((o, ex) -> ex.getCause() instanceof SQLException)
            .retryIf(ret -> ((Long) ret) != 1);
    @Before
    public void setup() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem://test");
        config.setMaximumPoolSize(32);
        config.setMinimumIdle(32);
        hikariCP = new HikariPool(config);
        try (Connection connection = hikariCP.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE emp (" +
                    "emp_id BIGINT," +
                    "name VARCHAR(200)" +
                    ")");
        }
    }

    Function<Long, Runnable> genInsertTask = (id) -> () -> {
        try(Connection conn = hikariCP.getConnection();
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO emp(emp_id, name) VALUES(?,?)")) {
            stmt.setLong(1, id);
            stmt.setString(2, "xxx");
            stmt.executeUpdate();
            conn.commit();
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    };

    Function<Long, Callable<Long>> genSelectTask = (id) -> () -> {
        try(Connection conn = hikariCP.getConnection();
            PreparedStatement stmt = conn.prepareStatement("SELECT count(*) FROM emp WHERE emp_id=?")) {
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch(SQLException e) {
            LOG.error("{}", e.getMessage());
            throw new RuntimeException(e);
        }
    };

    Function<Long, Callable<Long>> genFailsafeSelectTask = (id) -> () -> {
        Callable<Long> selectTask = genSelectTask.apply(id);
        return Failsafe.with(selectRetryPolicy)
                .get(() -> selectTask.call());
    };
    @Test
    public void test() throws SQLException, InterruptedException {
        ExecutorService service = Executors.newScheduledThreadPool(16);

        for (int i=0; i<10000; i++) {
            long id = i;
            service.submit(genInsertTask.apply(id));
            service.submit(genFailsafeSelectTask.apply(id));
        }

        service.shutdown();
        service.awaitTermination(1, TimeUnit.MINUTES);
    }
}
