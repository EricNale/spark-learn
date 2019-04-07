package com.lucas.boot.job;

import lombok.AllArgsConstructor;

/**
 * StatisticEnum
 *
 * @author yupeng10@baidu.com
 * @since 1.0
 */
@AllArgsConstructor
public enum StatisticEnum {


    /**
     * 查询模板, 输入集合, 输出集合
     */
//    SCENE_PVUV_PERSON("scene-pvuv-person-condition.vm",
//            "mongodb://iss:" + URLEncoder.encode("iss_MONGO#2017#@") + "@10.90.222.13:8765/apiproddb.CollectionLog",
//            "mongodb://localhost:10051/yupeng.statistic_scene_pv_uv_person"),

    SCENE_PV_UV_PERSON("scene-pvuv-person-condition.vm",
            "mongodb://localhost:10051/yupeng.CollectionLog",
            "mongodb://localhost:10051/yupeng.statistic_scene_pv_uv_person"),
    SCENE_AVG_STAYTIME_PERSON("scene-pvuv-person-condition.vm",
            "mongodb://localhost:10051/yupeng.CollectionLog",
            "mongodb://localhost:10051/yupeng.statistic_scene_avg_staytime_person"),
    SCENE_PV_UV_STATISTIC("scene-pvuv-condition.vm",
            "mongodb://localhost:10051/yupeng.statistic_scene_pv_uv_person",
            "mongodb://localhost:10051/yupeng.statistic_scene_pv_uv"),


    SYNC("",
            "mongodb://localhost:10051/sv.face_collectionlog_2",
            "mongodb://localhost:10051/sv.face_collectionlog_1");


    private String match;
    private String input;
    private String output;

    public String getMatch() {
        return match;
    }

    public String getInput() {
        return input;
    }

    public String getOutput() {
        return output;
    }
}
