package com.lucas.boot.spark.utils;

import com.google.common.collect.Maps;
import com.lucas.boot.job.MongoConnect;
import com.lucas.boot.spark.vo.ConditionQueryVO;
import com.lucas.boot.spark.vo.SceneQueryVO;
import com.mongodb.BasicDBObject;
import org.apache.spark.sql.SparkSession;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.data.mongodb.core.query.Criteria;

import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * SparkUtils
 *
 * @author yupeng10@baidu.com
 * @since 1.0
 */
public class SparkUtils {

    public static SparkSession buildSession(String inputUrl, String outputUrl) {
        return SparkSession.builder().appName("local").master("local")
                .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
                .config("spark.mongodb.input.uri", inputUrl)
                .config("spark.mongodb.output.uri", outputUrl)
                .getOrCreate();
    }


    public static SparkSession buildMongoSpark() {
        String password = "iss_MONGO#2017#@";
        String remoteUri = "mongodb://iss:" + URLEncoder.encode(password) + "@10.90.222.13:8765/apiproddb.CollectionLog";
        String inputUrl = "mongodb://localhost:10051/yupeng.sparkLog";
        String outputUrl = "mongodb://localhost:10051/yupeng.sparkLogOutput";
        return SparkSession.builder().appName("local").master("local")
                .config("spark.mongodb.input.uri", remoteUri)
                .config("spark.mongodb.output.uri", inputUrl)
                .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
                .getOrCreate();
    }


    public static SparkSession buildMongoSpark(MongoConnect input, MongoConnect output) {

        String inputUrl = getUrl(input);
        String outputUrl = getUrl(output);

        return SparkSession.builder().appName("local").master("local")
                .config("spark.mongodb.input.uri", inputUrl)
                .config("spark.mongodb.output.uri", outputUrl)
                .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
                .getOrCreate();

    }

    private static String getUrl(MongoConnect input) {
        return "mongodb://" + input.getUsername() + ":" + URLEncoder.encode(input.getPassword()) + "@" + input.getHost() + "/" + input.getDbName() + "." + input.getCollection();
    }


    public static Map<String, Object> buildCondition(Object queryVO) {

        return beanToMap(queryVO);
    }

    public static <T> Map<String, Object> beanToMap(T bean) {
        Map<String, Object> map = Maps.newHashMap();
        if (bean != null) {
            BeanMap beanMap = BeanMap.create(bean);
            for (Object key : beanMap.keySet()) {
                if (beanMap.get(key) != null) {
                    map.put(key + "", beanMap.get(key));
                }
            }
        }
        return map;
    }

    public static Criteria buildCriteria(ConditionQueryVO vo) {
        SceneQueryVO queryVO = (SceneQueryVO) vo;
        Criteria criteria = new Criteria();
        if (queryVO.getCollectionId() != null) {
            criteria.and("collectionId").equals(queryVO.getCollectionId());
        }
        if (queryVO.getStartTime() != null) {
            criteria.and("snapshotTimel").gte(queryVO.getStartTime()).lt(queryVO.getEndTime());
        }
        return criteria;
    }


    public static <T> BasicDBObject bean2DBObject(T bean) throws IllegalArgumentException, IllegalAccessException {
        if (bean == null) {
            return null;
        }
        BasicDBObject dbObject = new BasicDBObject();
        // 获取对象对应类中的所有属性域
        Field[] fields = bean.getClass().getDeclaredFields();
        for (Field field : fields) {
            // 获取属性名
            String varName = field.getName();
            // 修改访问控制权限
            boolean accessFlag = field.isAccessible();
            if (!accessFlag) {
                field.setAccessible(true);
            }
            Object param = field.get(bean);
            if (param == null) {
                continue;
            } else if (param instanceof Integer) {// 判断变量的类型
                int value = ((Integer) param).intValue();
                dbObject.put(varName, value);
            } else if (param instanceof String) {
                String value = (String) param;
                dbObject.put(varName, value);
            } else if (param instanceof Double) {
                double value = ((Double) param).doubleValue();
                dbObject.put(varName, value);
            } else if (param instanceof Float) {
                float value = ((Float) param).floatValue();
                dbObject.put(varName, value);
            } else if (param instanceof Long) {
                long value = ((Long) param).longValue();
                dbObject.put(varName, value);
            } else if (param instanceof Boolean) {
                boolean value = ((Boolean) param).booleanValue();
                dbObject.put(varName, value);
            } else if (param instanceof Date) {
                Date value = (Date) param;
                dbObject.put(varName, value);
            } else if (param instanceof List) {
                List<Object> list = (List<Object>) param;
                dbObject.put(varName, list);
            } else if (param instanceof Map) {
                Map<Object, Object> map = (Map<Object, Object>) param;
                dbObject.put(varName, map);
            }
            // 恢复访问控制权限
            field.setAccessible(accessFlag);
        }
        return dbObject;
    }


}
