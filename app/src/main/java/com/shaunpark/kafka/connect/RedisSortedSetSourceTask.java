package com.shaunpark.kafka.connect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import com.shaunpark.kafka.connect.redis.RedisClient;


public class RedisSortedSetSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(RedisSortedSetSourceTask.class);
    public static final String REDIS_KEY = "REDIS_KEY";
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String topic;
    private int batchSize;
    private RedisClient client;
    private String redisKey;
    private int pollInterval;

    private Long streamOffset;
    private Time time;
    
    public RedisSortedSetSourceTask() {
    }

    @Override
    public String version() {
        return new RedisSortedSetSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(RedisSortedSetSourceConnector.CONFIG_DEF, props);
        String hostName = config.getString(RedisSortedSetSourceConnector.REDIS_HOST_CONFIG);
        int portNumber = config.getInt(RedisSortedSetSourceConnector.REDIS_PORT_CONFIG);
        pollInterval = config.getInt(RedisSortedSetSourceConnector.POLL_MAX_INTERVAL_MS_CONFIG);

        redisKey = config.getString(RedisSortedSetSourceConnector.REDIS_KEY_CONFIG);
        topic = config.getString(RedisSortedSetSourceConnector.TOPIC_CONFIG);
        batchSize = config.getInt(RedisSortedSetSourceConnector.TASK_BATCH_SIZE_CONFIG);
        time = new SystemTime();

        client = new RedisClient(hostName, portNumber, batchSize, redisKey);
        client.connect();
    }


    // sorted set에서 데이터 조회
    // 
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        streamOffset = System.currentTimeMillis();
        log.info("Poll started : " + streamOffset);
        List<String>data = client.getSendData(streamOffset);

        List<SourceRecord> records = new ArrayList<SourceRecord>();
        log.info("polled records count " + data.size());
        if( data.size() <= 0 ) {
            time.sleep(pollInterval);
        } else {
            for ( String message : data) {
                records.add(new SourceRecord(offsetKey(redisKey), offsetValue(streamOffset), topic, null,
                null, null, VALUE_SCHEMA, message, System.currentTimeMillis()));
            }
        }

        return records;
    }

    // delete sent records from REDIS
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        try {
            client.delete((String)record.value());
        } catch(Exception e) {
            log.error("error in commitRecord", e);
        }
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            if (client != null ) {
                client.close();
                log.trace("Closed redis connection");
            }
            this.notify();
        }
    }

    private Map<String, String> offsetKey(String key) {
        return Collections.singletonMap(REDIS_KEY, key);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }
}