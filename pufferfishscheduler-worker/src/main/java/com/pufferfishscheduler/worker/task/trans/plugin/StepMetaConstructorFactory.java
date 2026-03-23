package com.pufferfishscheduler.worker.task.trans.plugin;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.worker.task.trans.plugin.constructor.TableInputConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.constructor.TableOutputConstructor;

/**
 * 组件元数据构造工厂
 */
public class StepMetaConstructorFactory {
    public static AbstractStepMetaConstructor getConstructor(String type) {
        switch (type) {
            // case Constants.StepMetaType.EXCEL_INPUT:
            // return new ExcelInputConstructor();
            // case Constants.StepMetaType.EXCEL_OUTPUT:
            // return new ExcelOutputConstructor();
            case Constants.StepMetaType.TABLE_INPUT:
                return new TableInputConstructor();
            case Constants.StepMetaType.TABLE_OUTPUT:
                return new TableOutputConstructor();
            // case Constants.StepMetaType.JSON_INPUT:
            //     return new JsonInputConstructor();
            // case Constants.StepMetaType.JSON_OUTPUT:
            //     return new JsonOutputConstructor();
            // case Constants.StepMetaType.CSV_INPUT:
            //     return new CsvInputConstructor();
            // case Constants.StepMetaType.GENERATE_TEST_DATA:
            //     return new GenerateTestDataConstructor();
            // case Constants.StepMetaType.EXECUTE_SQL_SCRIPT:
            //     return new ExecuteSqlConstructor();
            // case Constants.StepMetaType.JAVA_CODE:
            //     return new JavaCodeConstructor();
            // case Constants.StepMetaType.GET_VARIABLES:
            //     return new GetVariablesConstructor();
            // case Constants.StepMetaType.SET_VARIABLES:
            //     return new SetVariablesConstructor();
            // case Constants.StepMetaType.CONDITIONAL_JAVA:
            //     return new ConditionalJavaConstructor();
            // case Constants.StepMetaType.MONGODB_INPUT:
            //     return new MongoDBInputConstructor();
            // case Constants.StepMetaType.INSERT_OR_UPDATE:
            //     return new InsertOrUpdateConstructor();
            // case Constants.StepMetaType.UPDATE:
            //     return new UpdateConstructor();
            // case Constants.StepMetaType.FIELD_SELECT:
            //     return new FieldSelectConstructor();
            // case Constants.StepMetaType.MONGODB_OUTPUT:
            //     return new MongoDBOutputConstructor();
            // case Constants.StepMetaType.FORMULA:
            //     return new FormulaConstructor();
            // case Constants.StepMetaType.PROCESS_BRANCH:
            //     return new ProcessBranchConstructor();
            // case Constants.StepMetaType.API_INPUT:
            //     return new ApiInputConstructor();
            // case Constants.StepMetaType.API_OUTPUT:
            //     return new ApiOutputConstructor();
            // case Constants.StepMetaType.FILE_DOWNLOAD:
            //     return new FileDownloadConstructor();
            // case Constants.StepMetaType.KAFKA_CONSUMER_INPUT:
            //     return new KafkaConsumerInputConstructor();
            // case Constants.StepMetaType.KAFKA_PRODUCER_OUTPUT:
            //     return new KafkaProducerOutputConstructor();
            // case Constants.StepMetaType.RECORDS_FROM_STREAM:
            //     return new RecordsFromStreamConstrustor();
            // case Constants.StepMetaType.SYSTEM_DATE:
            //     return new SystemDateConstructor();
            // case Constants.StepMetaType.WRITE_TO_LOG:
            //     return new WriteToLogConstructor();
            // case Constants.StepMetaType.REDIS_INPUT:
            //     return new RedisInputConstructor();
            // case Constants.StepMetaType.REDIS_OUTPUT:
            //     return new RedisOutputConstructor();
            // case Constants.StepMetaType.DENORMALISER:
            //     return new DenormaliserConstructor();
            // case Constants.StepMetaType.NORMALISER:
            //     return new NormaliserConstructor();
            // case Constants.StepMetaType.SPLITFIELDTOROWS3:
            //     return new SplitFieldToRows3Constructor();
            // case Constants.StepMetaType.FIELDSPLITTER:
            //     return new FieldSplitterConstructor();
            // case Constants.StepMetaType.GROUPBY:
            //     return new GroupByConstructor();
            // case Constants.StepMetaType.FILTER:
            //     return new FilterConstructor();
            // case Constants.StepMetaType.DATA_CLEAN:
            //     return new DataCleanConstructor();
            // case Constants.StepMetaType.STARROCKS_OUTPUT:
            //     return new StarRocksOutputConstructor();
            // case Constants.StepMetaType.DEBEZIUM_JSON:
            //     return new DebeziumJsonConstructor();
            // case Constants.StepMetaType.DORIS_OUTPUT:
            //     return new DorisOutputConstructor();
            // case Constants.StepMetaType.FTP_UPLOAD:
            //     return new FTPUploadConstructor();
            // case Constants.StepMetaType.FTP_DOWNLOAD:
            //     return new FTPDownloadConstructor();
            default:
                throw new BusinessException(String.format("暂不支持此类型组件![type:%s]", type));
        }
    }
}
