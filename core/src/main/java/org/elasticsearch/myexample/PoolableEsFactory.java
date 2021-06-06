package org.elasticsearch.myexample;

import org.apache.commons.pool.BasePoolableObjectFactory;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class PoolableEsFactory extends BasePoolableObjectFactory<EsClient> {

    private EsEnv esEnv = null;

    /**
     * 日志类
     */
    protected final Logger logger = LogManager.getLogger(PoolableEsFactory.class.getName());

    public PoolableEsFactory(EsEnv esEnv) {
        super();
        this.esEnv = esEnv;
    }

    @Override
    public EsClient makeObject() throws Exception {
        EsClient esClient = new EsClient(esEnv);
        logger.info("create EsClient:{}! ", esClient);
        return esClient;
    }

    @Override
    public void destroyObject(EsClient esClient) throws Exception {
        esClient.close();
        logger.info("destroyObject EsClient:{}! ", esClient);
        super.destroyObject(esClient);
    }

    @Override
    public void passivateObject(EsClient esClient) throws Exception {
        logger.info("return to pool");
        super.passivateObject(esClient);
    }

}
