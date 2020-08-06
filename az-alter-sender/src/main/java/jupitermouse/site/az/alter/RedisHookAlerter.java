package jupitermouse.site.az.alter;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerException;
import azkaban.sla.SlaOption;
import azkaban.utils.Props;
import com.google.gson.Gson;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static jupitermouse.site.az.alter.Key.ALERTER_REDIS_DB;
import static jupitermouse.site.az.alter.Key.ALERTER_REDIS_HOST;
import static jupitermouse.site.az.alter.Key.ALERTER_REDIS_KEY;
import static jupitermouse.site.az.alter.Key.ALERTER_REDIS_PASSWORD;
import static jupitermouse.site.az.alter.Key.ALERTER_REDIS_PORT;
import static jupitermouse.site.az.alter.Key.ALTER_EVENT_NOTIFY;

/**
 * <p>
 * Redis 预警接收
 * </p>
 *
 * @author JupiterMouse 2020/8/6
 * @since 1.0
 */
@Singleton
public class RedisHookAlerter implements Alerter {

    private static final Logger logger = Logger.getLogger(RedisHookAlerter.class);

    private final Props props;

    private static final Gson GSON = new Gson();

    private static final String EMPTY = "";

    private static final FastDateFormat DEFAULT_DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    @Inject
    public RedisHookAlerter(final Props props) {
        this.props = props;
    }

    @Override
    public void alertOnSuccess(ExecutableFlow exflow) throws Exception {
        // noting to do
    }

    @Override
    public void alertOnError(ExecutableFlow exflow, String... extraReasons) throws Exception {
        logger.info("Execute Workflow Error , Alter Start... ");
        // 一个是任务重试次数完毕后的失败告警
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        this.putAlterInfo(exflow, map);
        this.getRedisConnection(props).lpush(props.getString(ALERTER_REDIS_KEY, ALTER_EVENT_NOTIFY), GSON.toJson(map));
        map.put("extraReasons", Arrays.toString(extraReasons));
        this.getRedisConnection(props).lpush(props.getString(ALERTER_REDIS_KEY, ALTER_EVENT_NOTIFY), GSON.toJson(map));
    }

    @Override
    public void alertOnFirstError(ExecutableFlow exflow) throws Exception {
        logger.info("Execute Workflow Error First, Alter Start... ");
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        // 第一个失败告警
        this.putAlterInfo(exflow, map);
        this.getRedisConnection(props).lpush(props.getString(ALERTER_REDIS_KEY, ALTER_EVENT_NOTIFY), GSON.toJson(map));
    }

    @Override
    public void alertOnSla(SlaOption slaOption, String slaMessage) throws Exception {

    }

    @Override
    public void alertOnFailedUpdate(Executor executor, List<ExecutableFlow> executions, ExecutorManagerException e) {

    }

    @Override
    public void alertOnFailedExecutorHealthCheck(Executor executor, List<ExecutableFlow> executions,
            ExecutorManagerException e, List<String> alertEmails) {

    }

    private Jedis getRedisConnection(Props props) {
        String host = props.getString(ALERTER_REDIS_HOST, "127.0.0.1");
        int port = props.getInt(ALERTER_REDIS_PORT, 6379);
        int db = props.getInt(ALERTER_REDIS_DB, 1);
        JedisPool pool = new JedisPool(host, port);
        Jedis jedis = pool.getResource();
        Optional.of(props.getString(ALERTER_REDIS_PASSWORD, EMPTY)).ifPresent(jedis::auth);
        jedis.select(db);
        return jedis;
    }

    private void putAlterInfo(ExecutableFlow exflow, Map<String, String> store) {
        store.put("status", exflow.getStatus().name());
        store.put("flowId", exflow.getFlowId());
        store.put("executionId", String.valueOf(exflow.getExecutionId()));
        store.put("startTime", DEFAULT_DATE_FORMAT.format(exflow.getStartTime()));
        store.put("endTime", DEFAULT_DATE_FORMAT.format(exflow.getEndTime()));
    }

}
