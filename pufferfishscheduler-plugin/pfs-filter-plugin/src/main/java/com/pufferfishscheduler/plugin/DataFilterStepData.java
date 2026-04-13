package com.pufferfishscheduler.plugin;

import java.util.List;
import org.codehaus.janino.ExpressionEvaluator;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * 数据过滤步骤数据
 */
public class DataFilterStepData extends BaseStepData implements StepDataInterface {
    public RowMetaInterface outputRowMeta;
    public ExpressionEvaluator expressionEvaluator;
    public Object[] argumentData;
    public List<Integer> argumentIndexes;
}
