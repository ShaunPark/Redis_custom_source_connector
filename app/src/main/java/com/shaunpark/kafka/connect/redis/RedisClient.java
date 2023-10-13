package com.shaunpark.kafka.connect.redis;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisClient {
    private String key;
    private JedisPool pool;
    private String host;
    private int port;
    private int batchSize;

    private static final Logger log = LoggerFactory.getLogger(RedisClient.class);
    private final static Hashtable<String,Long> producedMessages = new Hashtable<String,Long>();
    private static long cleanTime = 0;

    public RedisClient(String hostName, int port, int batchSize, String key) {
        this.host = hostName;
        this.port = port;
        this.key = key;
        this.batchSize = batchSize;
    }

    private int retryCount = 10;
    public void connect() {
        if(pool == null || pool.isClosed()) {
            while(true) {
                try {
                    pool = new JedisPool(host, port);
                    retryCount = 10;
                    break;
                } catch( JedisConnectionException je) {
                    if ( retryCount > 0 ) {
                        retryCount--;
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            break;
                        }
                    } else {
                        throw je;
                    }
                }
            } 
            retryCount = 10;
        }
    }

    private static void clean() {
        long nowMs = System.currentTimeMillis();
        if (cleanTime == 0 ) {
            cleanTime = nowMs;
        }
        if( nowMs - cleanTime > 600000  ) { // 10 mins
            Enumeration<String> msgKeys =  producedMessages.keys();
            while(msgKeys.hasMoreElements()) {
                String msgKey = msgKeys.nextElement();
                long ms = producedMessages.get(msgKey);
                if ( nowMs - ms > 3600000 ) {
                    producedMessages.remove(msgKey);
                }
            }
        }
    }
    
    public List<String> getSendData(long endPosition) {
        connect();
        long pollTime = System.currentTimeMillis();
        ArrayList<String> newData = new ArrayList<String>();
        try (Jedis jedis = pool.getResource()) {
            List<String> list = jedis.zrangeByScore(key, 0, endPosition, 0, batchSize);
            for( String msg : list) {
                if( producedMessages.containsKey(msg)) {
                    continue;
                } else {
                    newData.add(msg);
                    producedMessages.put(msg, pollTime);
                }
            }
            return newData;
        } catch(JedisConnectionException ce) {
            log.error("Error while getData. reconnect on next poll()", ce);
            return new ArrayList<String>();
        } finally {
            clean();
        }
    }

    private List<String> shouldDeleted = new ArrayList<String>();
    public void delete(String deleteItems) {
        connect();

        try (Jedis jedis = pool.getResource()) {
            for( String item : shouldDeleted) {
                jedis.zrem(key, deleteItems);
                shouldDeleted.remove(item);
            }

            if( producedMessages.containsKey(deleteItems)) {
                producedMessages.remove(deleteItems);
                log.info("commitRecord : delete processed record" + deleteItems);
                jedis.zrem(key, deleteItems);
            }
        } catch(JedisConnectionException ce) {
            log.error("Error while delete.", ce);
        }
    }

    public void close() {
        if(pool != null && !pool.isClosed())
            pool.close();
    }
}
