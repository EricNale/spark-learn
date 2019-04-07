package com.lucas.boot.spark.vo;

import java.util.List;

/**
 * SceneQueryVO
 *
 * @author yupeng10@baidu.com
 * @since 1.0
 */
public class SceneQueryVO extends ConditionQueryVO {

    private Long sceneId;
    private List<Long> enterCameraIds;
    private List<Long> outerCameraIds;
    private Long collectionId;

    public List<Long> getEnterCameraIds() {
        return enterCameraIds;
    }

    public void setEnterCameraIds(List<Long> enterCameraIds) {
        this.enterCameraIds = enterCameraIds;
    }

    public List<Long> getOuterCameraIds() {
        return outerCameraIds;
    }

    public void setOuterCameraIds(List<Long> outerCameraIds) {
        this.outerCameraIds = outerCameraIds;
    }

    public Long getCollectionId() {
        return collectionId;
    }

    public void setCollectionId(Long collectionId) {
        this.collectionId = collectionId;
    }

    public Long getSceneId() {
        return sceneId;
    }

    public void setSceneId(Long sceneId) {
        this.sceneId = sceneId;
    }

}
