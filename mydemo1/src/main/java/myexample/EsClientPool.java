package myexample;

import org.apache.commons.pool.impl.StackObjectPool;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

/**
 * ClassName:EsClientPool <br/>
 * Date:     2015年11月23日 下午5:13:12 <br/>
 * @author   qiyongkang
 * @version  
 * @since    JDK 1.6
 * @see      
 */
public class EsClientPool {
    /**
     * 连接池
     */
    private StackObjectPool<EsClient> pool = null;

    /**
     * 日志类
     */
//    private Logger logger = LogManager.getLogger(EsClientPool.class.getName());

    /**
     * 
     * Creates a new instance of EsClientPool.
     *
     * @param clusterName
     * @param ip
     * @param port
     * @param keepClienNum
     */
    public EsClientPool(String clusterName, String ip, String port, int keepClienNum) {
        EsEnv esEnv = new EsEnv(clusterName, ip, port);

        this.pool =  new StackObjectPool<EsClient>(new PoolableEsFactory(esEnv), keepClienNum);

//        this.logger.info("初始化一个es pool成功...");
    }

    /**
     * 
     * getEsClient:获取一个es客户端. <br/>
     *
     * @author qiyongkang
     * @return
     * @since JDK 1.6
     */
    public EsClient getEsClient(){
        EsClient esClient = null;
        try {
            esClient = pool.borrowObject();
        } catch (Exception e) {
//            logger.error("create Client error!" , e);
        }
        return esClient;
    }

    /**
     * 
     * removeEsClient:移除一个es客户端. <br/>
     *
     * @author qiyongkang
     * @param esClient
     * @return
     * @since JDK 1.6
     */
    public EsClient removeEsClient(EsClient esClient){
        try {
            pool.returnObject(esClient);
        } catch (Exception e) {
//            logger.error("Client return to pool error!" , e);
        }
        return esClient;
    }
}
