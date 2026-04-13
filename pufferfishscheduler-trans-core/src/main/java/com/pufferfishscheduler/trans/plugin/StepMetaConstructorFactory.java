package com.pufferfishscheduler.trans.plugin;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.trans.plugin.constructor.*;

/**
 * 组件元数据构造工厂
 */
public class StepMetaConstructorFactory {
    public static AbstractStepMetaConstructor getConstructor(String type) {
        return switch (type) {
            case Constants.StepMetaType.TABLE_INPUT -> new TableInputConstructor();
            case Constants.StepMetaType.TABLE_OUTPUT -> new TableOutputConstructor();
            case Constants.StepMetaType.EXCEL_INPUT -> new ExcelInputConstructor();
            case Constants.StepMetaType.EXCEL_OUTPUT -> new ExcelOutputConstructor();
//            case Constants.StepMetaType.CSV_INPUT -> new CsvInputConstructor();
//            case Constants.StepMetaType.JSON_INPUT -> new JsonInputConstructor();
//            case Constants.StepMetaType.JSON_OUTPUT -> new JsonOutputConstructor();
            case Constants.StepMetaType.ROW_GENERATOR -> new RowGeneratorConstructor();
            case Constants.StepMetaType.SQL_SCRIPT -> new SQLScriptConstructor();
            case Constants.StepMetaType.JAVA_CODE -> new JavaCodeConstructor();
//            case Constants.StepMetaType.GET_VARIABLES -> new GetVariablesConstructor();
//            case Constants.StepMetaType.SET_VARIABLES -> new SetVariablesConstructor();
            case Constants.StepMetaType.JAVA_CONDITIONAL -> new JavaConditionConstructor();
            case Constants.StepMetaType.PROCESS_BRANCH -> new ProcessBranchConstructor();
            case Constants.StepMetaType.INSERT_OR_UPDATE -> new InsertOrUpdateConstructor();
            case Constants.StepMetaType.FIELD_SELECT -> new FieldSelectConstructor();
//            case Constants.StepMetaType.MONGODB_INPUT -> new MongoDBInputConstructor();
            case Constants.StepMetaType.API_INPUT -> new ApiInputConstructor();
            case Constants.StepMetaType.API_OUTPUT -> new ApiOutputConstructor();
            case Constants.StepMetaType.FILE_DOWNLOAD -> new FileDownloadConstructor();
            case Constants.StepMetaType.REDIS_INPUT -> new RedisInputConstructor();
            case Constants.StepMetaType.REDIS_OUTPUT -> new RedisOutputConstructor();
            case Constants.StepMetaType.KAFKA_CONSUMER_INPUT -> new KafkaConsumerInputConstructor();
            case Constants.StepMetaType.KAFKA_PRODUCER_OUTPUT -> new KafkaProducerOutputConstructor();
            case Constants.StepMetaType.RABBITMQ_CONSUMER_INPUT -> new RabbitMqConsumerInputConstructor();
            case Constants.StepMetaType.RABBITMQ_PRODUCER_OUTPUT -> new RabbitMqProducerOutputConstructor();
            case Constants.StepMetaType.RECORDS_FROM_STREAM -> new RecordsFromStreamConstructor();
            case Constants.StepMetaType.SYSTEM_DATE -> new SystemDateConstructor();
            case Constants.StepMetaType.WRITE_TO_LOG -> new WriteToLogConstructor();
            case Constants.StepMetaType.DENORMALIZED -> new DenormalizedConstructor();
            case Constants.StepMetaType.NORMALISER -> new NormaliserConstructor();
            case Constants.StepMetaType.SORT_ROWS -> new SortRowsConstructor();
            case Constants.StepMetaType.SPLIT_FIELD_TO_ROWS -> new SplitFieldToRowsConstructor();
            case Constants.StepMetaType.FIELD_SPLIT_TO_COLUMNS -> new FieldSplitterConstructor();
            case Constants.StepMetaType.DATA_FILTER -> new DataFilterConstructor();
            case Constants.StepMetaType.DATA_CLEAN -> new DataCleanConstructor();
            case Constants.StepMetaType.DEBEZIUM_JSON -> new DebeziumJsonConstructor();
            case Constants.StepMetaType.DORIS_OUTPUT -> new DorisOutputConstructor();
            case Constants.StepMetaType.FTP_UPLOAD -> new FTPUploadConstructor();
            case Constants.StepMetaType.FTP_DOWNLOAD -> new FTPDownloadConstructor();
            default -> throw new BusinessException(String.format("暂不支持此类型组件![type:%s]", type));
        };
    }
}
