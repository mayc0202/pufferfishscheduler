package com.pufferfishscheduler.common.dict.service;

import com.pufferfishscheduler.domain.vo.dict.DictItem;

import java.util.List;

public interface DictService {

    /**
     * 获取字典
     *
     * @param dictCode
     * @return
     */
    List<DictItem> getDict(String dictCode);

    /**
     * 获取字典项code
     *
     * @param dictCode
     * @param itemValue
     * @return
     */
    String getDictItemCode(String dictCode, String itemValue);
}
