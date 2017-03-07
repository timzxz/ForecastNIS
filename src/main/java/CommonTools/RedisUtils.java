/**
 * @Project:WebCrawler
 * @Title:RedisConnectionSource.java
 * @Description:TODO
 * @autor:wing
 * @date: @2016-5-26下午1:16:36
 * @Copyright:2016 hit. All rights reserved.
 * @version:V1.0
 */
package CommonTools;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName RedisConnectionSource
 * @Description 利用redis进行URL去重
 * @author wing
 * @date 2016-5-26下午1:16:36
 */
public class RedisUtils {

	   //Redis服务器IP
    private static String ADDR = "8.8.8.8";
    
    //Redis的端口号
    private static int PORT = 6666;
    
    //访问密码
//    private static String AUTH = "******";
    
    //可用连接实例的最大数目，默认值为8；
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static int MAX_ACTIVE = 1024;
    
    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static int MAX_IDLE = 200;
    
    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static int MAX_WAIT = 100000;
    
    private static int TIMEOUT = 100000;
    
    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static boolean TEST_ON_BORROW = true;
    
    private static JedisPool jedisPool = null;

	

	
	static {
        try {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(MAX_ACTIVE);
            config.setMaxIdle(MAX_IDLE);
            config.setMaxWaitMillis(MAX_WAIT);
            config.setTestOnBorrow(TEST_ON_BORROW);
            jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	/**
     * 获取Jedis实例
     * @return
     */
    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    /**
     * 释放jedis资源
     * @param jedis
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);
        }
    }
	/**
	 * 
	 * @Description:判断url是否已经存在，存在返回true,不存在返回fasle
	 * @User:wing
	 * @Updatetime:2016-5-26下午1:48:49
	 *
	 */
	public static synchronized boolean isContains(String table,String url){
		Jedis jedis = getJedis();
		boolean re = jedis.sismember(table, url);
		returnResource(jedis);
		return re;
	}
	public static synchronized boolean isContains(String url){
		Jedis jedis = getJedis();
		boolean re = jedis.sismember("url", url);
		returnResource(jedis);
		return re;
	}
	
	/**
	 * 
	 * @Description:向set中添加URL
	 * @User:wing
	 * @Updatetime:2016-5-26下午1:49:28
	 *
	 */
	public static void add(String table,String url){
		Jedis jedis = getJedis();
		jedis.sadd(table, url);
		returnResource(jedis);
	}
	public static void add(String url){
		Jedis jedis = getJedis();
		jedis.sadd("url", url);
		returnResource(jedis);
	}
	//移除
	public static void remove(String collection,String url){
		Jedis jedis = getJedis();
		jedis.srem(collection, url);
		returnResource(jedis);
	}
	
	//返回集合元素个数
	public static Long getCount(String collection){
		Jedis jedis = getJedis();
		Long count = jedis.scard(collection);
		returnResource(jedis);
		return count;
		
	}
	
	//返回集合元素个数
	public static Set<String> getElements(String collection){
		Jedis jedis = getJedis();
		Set<String> set = jedis.smembers(collection);
		returnResource(jedis);
		return set;
		
	}
	
	//清空某集合中的数据
	
	public static void del(String collection){
		Jedis jedis = getJedis();
		jedis.del(collection);
		returnResource(jedis);
		System.out.println("清空成功");
	}
	
	public static void main(String ...strings){
		String collection = "twitterHisTwi";
		RedisUtils.del(collection);
		
		
/*		Set<String> set = getElements(collection);
		for(String s:set){
			System.out.println(s);
		}*/
		
	}
	
	
	
	
	/*				
	if(RedisUtils.isContains("url",url)){
		System.out.println(url+"------处理过");
		currentPage++;	
		continue;
	}
	System.out.println(url);
	
	RedisUtils.add("url",url);*/

}
