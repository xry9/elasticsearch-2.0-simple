package com.xry9.elasticsearch;

//import com.qiyongkang.es.util.DateUtil;
//import com.qiyongkang.es.util.Page;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import java.util.Date;

/**
 * ClassName:TransportClientTest <br/>
 * Date:     2015年11月20日 上午9:47:45 <br/>
 * @author   qiyongkang
 * @version
 * @since    JDK 1.6
 * @see
 */
public class TransportClientTest {
    /**
     * 日志类
     */
//    private static Logger logger = LogManager.getLogger(TransportClientTest.class.getName());
//    https://blog.csdn.net/qiyongkang520/article/details/50225213?locationNum=8&fps=1
// http://localhost:9200/_cat/indices?v
    public static void main(String[] args) {
        EsClientPool pool = null;
        EsClient esclient = null;
        try {
            //初始化一个pool
            pool = new EsClientPool("elasticsearch", "127.0.0.1", "9500", 20);
//            logger.info("初始化一个es pool成功：{}", pool);

            //获取一个客户端
            esclient = pool.getEsClient();
//            logger.info("获取一个客户端：{}", esclient);

            //创建一个EsUserImpl实现类
            EsUserService esUserService = new EsUserImpl(esclient);
//            logger.info("创建一个user服务类：{}", esUserService);

            //操作开始时间：
            Date startTime = new Date();
            long startMilli = startTime.getTime();
//            logger.info("操作开始时间：{}", DateUtil.dateToYMDHMSStr(startTime));

            /***************索引********************/
            //创建一个索引
//            createIndex(esclient, "usermanage");

            //删除一个索引
//            deleteIndex(esclient, "usermanage1");
//
//            //关闭一个索引
//            closeIndex(esclient, "usermanage");
//
//            //开启一个索引
//            openIndex(esclient, "um");
//
//            //判断一个索引名是否存在
////            isExistIndex(esclient, "um");
//
//            /***************索引的别名********************/
//            //创建一个索引的别名
//            createAlias(esclient, "usermanage", "um");
//
//            //删除一个索引的别名
//            deleteAlias(esclient, "usermanage", "um");
//
//            /***************Mapping********************/
//            //创建MappingJson
//            esUserService.createMappingJson();

            /***************添加********************/
            //单个添加
            esUserService.singleInsert(new User(26L, "qiyongkang", 12, 0, DateUtil.strToYMDHMSDate("2021-11-1 11:11:11")));
//
//            //批量添加
//            Date date = DateUtil.strToYMDHMSDate("2015-11-1 11:11:11");
//            Calendar calendar = Calendar.getInstance();
//            calendar.setTime(date);
//
//            List<User> userList = new ArrayList<User>();
//            for (int i = 1; i <= 100000; i++) {
//                User user = new User();
//                user.setId(Long.valueOf(i));
//                user.setName("qiyongkang" + i);
//                user.setAge(i);
//                user.setSex(i % 2 == 0 ? 0 : 1);
//                calendar.add(Calendar.HOUR_OF_DAY, 1);
//                user.setBirthday(calendar.getTime());
//                userList.add(user);
//            }
////            logger.info("用户集合大小：{}", userList.size());
//            esUserService.batchInsert(userList);
//
//            /***************更新********************/
            //单个更新
//            User user = new User();
//            user.setId(3L);
//            user.setBirthday(DateUtil.strToYMDHMSDate("1990-10-11 11:11:11"));
//            user.setAge(25);
//            user.setSex(1);
//            esUserService.singleUpdate(user);
//
//            //批量更新
//            List<User> updateList = new ArrayList<User>();
//            updateList.add(new User(1L, "qiyongkang1u", 254, 0, null));
//            updateList.add(new User(2L, "qiyongkang2u", 254, 0, null));
//            updateList.add(new User(3L, "qiyongkang3u", 254, 0, null));
//            esUserService.batchUpdate(updateList);
//
//            /***************查询********************/
//            //分页查询
//            //条件设置
//            User userCondition = new User();
////            user.setId(1L);
////            user.setSex(0);
////            user.setName("qiyongkang5555");
////            user.setAge(10);
////            user.setBirthday(DateUtil.strToYMDHMSDate("2015-11-25 11:11:11"));
////            user.setStartTime(DateUtil.strToYMDHMSDate("2015-11-25 11:11:11"));
////            user.setEndTime(DateUtil.strToYMDHMSDate("2015-11-25 22:11:11"));
////            user.setStartAge(12);
////            user.setEndAge(18);
//            Page<User> page = esUserService.getPageModel(userCondition, 1, 20);
////            logger.info("********user list***********");
//            for (User u : page.getRows()) {
////                logger.info(u);
//            }
////            logger.info("********user list***********");

            //根据id查实体
            User entityById = esUserService.getEntityById(1);
            System.out.println(entityById);
//
//            /***************删除********************/
//            //删除
//            esUserService.singleDelete(new KeyValue("id", "4"));
//
//            //操作结束时间
//            Date endTime = new Date();
//            long endMilli = endTime.getTime();
//            logger.info("操作结束时间：{}", DateUtil.dateToYMDHMSStr(endTime));
//            logger.info("总耗时：{}毫秒。", endMilli - startMilli);
        } catch (Exception e) {
            e.printStackTrace();
//            logger.error("es操作出错。", e);
        } finally {
            //关闭客户端
            if (esclient != null) {
                esclient.close();
                pool.removeEsClient(esclient);
            }
        }
    }

    /**
     *
     * isExistIndex:判断索引名是否存在. <br/>
     *
     * @author qiyongkang
     * @param client
     * @param indexName
     * @since JDK 1.6
     */
    public static void isExistIndex(EsClient client, String indexName) {
        boolean isExist = client.isExists(indexName);
//        logger.info("{}索引是否存在：{}", indexName, isExist ? "存在" : "不存在");
    }

    /**
     *
     * openIndex:开启索引. <br/>
     *
     * @author qiyongkang
     * @param client
     * @since JDK 1.6
     */
    public static void openIndex(EsClient client, String indexName) {
        client.openIndex(indexName);
//        logger.info("开启索引{}成功", indexName);
    }

    /**
     *
     * closeIndex:关闭索引. <br/>
     *
     * @author qiyongkang
     * @param client
     * @since JDK 1.6
     */
    public static void closeIndex(EsClient client, String indexName) {
        client.closeIndex(indexName);
//        logger.info("关闭索引{}成功", indexName);
    }

    /**
     *
     * deleteAlias:删除索引的别名. <br/>
     *
     * @author qiyongkang
     * @param client
     * @since JDK 1.6
     */
    public static void deleteAlias(EsClient client, String indexName, String aliasName) {
        client.deleteAlias(indexName, aliasName);
//        logger.info("删除索引{}的别名{}成功", indexName, aliasName);
    }

    /**
     *
     * createAlias:创建一个别名. <br/>
     *
     * @author qiyongkang
     * @param client
     * @since JDK 1.6
     */
    public static void createAlias(EsClient client, String indexName, String aliasName) {
        client.createAlias(indexName, aliasName);
//        logger.info("创建索引{}的别名{}成功", indexName, aliasName);
    }

    /**
     *
     * deleteIndex:删除索引. <br/>
     *
     * @author qiyongkang
     * @param client
     * @since JDK 1.6
     */
    public static void deleteIndex(EsClient client, String indexName) {
        client.deleteIndex(indexName);
//        logger.info("删除一个索引成功,索引名：{}", indexName);
    }

    /**
     *
     * createIndex:创建一个索引. <br/>
     *
     * @author qiyongkang
     * @since JDK 1.6
     */
    public static void createIndex(EsClient client, String indexName) {
        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
                .put("index.compress", true)
                .put("index.store.compress.stored", true)
                .put("index.store.compress.tv", true)
                .put("index.refresh_interval", "60s")
                .put("index.translog.flush_threshold_ops", "50000")
                .build();
        client.createIndex(settings, indexName);
//        logger.info("创建一个索引成功,索引名：{}", indexName);
    }

}
