package com.lucas.boot.spark.vo;

import lombok.Data;

import java.util.List;

/**
 * DataResourceQueryVO
 *
 * @author yupeng10@baidu.com
 * @since 1.0
 */
@Data
public class DataResourceQueryVO extends ConditionQueryVO {
    /**
     * 门店列表
     */
    private List<Long> sceneIds;
    /**
     * 分组id
     */
    private Long id;
}
