package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.AfterInjection;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

/**
 * DorisStreamLoaderMeta
 */
@Step( id = "DorisOutput", name = "Doris输出",
  description = "BaseStep.TypeTooltipDesc.DorisStreamLoader",
  categoryDescription = "输出",
  image = "doris.svg")
@InjectionSupported( localizationPrefix = "DorisStreamLoader.Injection.", groups = { "FIELDS" } )
public class DorisStreamLoaderMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DorisStreamLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  /** what's the schema for the target? */
  @Injection( name = "FENODES" )
  private String fenodes;

  /** The name of the FIFO file to create */
  @Injection( name = "DATABASE" )
  private String database;

  @Injection( name = "TABLE" )
  private String table;

  @Injection(name = "USERNAME")
  private String username;

  @Injection(name = "PASSWORD")
  private String password;

  private String streamLoadProp;

  private long bufferFlushMaxRows;

  private long bufferFlushMaxBytes;

  private int maxRetries;

  private boolean deletable;

  /** Field name of the target table */
  @Injection( name = "FIELD_TABLE", group = "FIELDS" )
  private String[] fieldTable;

  /** Field name in the stream */
  @Injection( name = "FIELD_STREAM", group = "FIELDS" )
  private String[] fieldStream;

  // 刷新频率
  private long scanningFrequency;

  // 连接超时
  private long connectTimeout;

  // Stream Load载入数据超时时间
  private long loadTimeout;

  // 合并方式
  private String mergeType;

  // 部分列更新
  private String partialColumns;

  // 数据库ip地址
  private String host;

  // 端口
  private String port;

  // 数据库类型
  private String databaseType;

  // 是否清空表数据
  private Boolean truncateTable;

  // 扩展配置
  private String extConfig;

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, databases );
  }

  private void readData( Node stepnode, List<? extends SharedObjectInterface> databases ) throws KettleXMLException {
    try {
      fenodes = XMLHandler.getTagValue(stepnode, "fenodes");
      database = XMLHandler.getTagValue(stepnode, "database");
      table = XMLHandler.getTagValue(stepnode, "table");
      username = XMLHandler.getTagValue(stepnode, "username");
      password = XMLHandler.getTagValue(stepnode, "password");
      if (password == null) {
        password = "";
      }

      bufferFlushMaxRows = Long.valueOf(XMLHandler.getTagValue(stepnode, "bufferFlushMaxRows"));
      bufferFlushMaxBytes = Long.valueOf(XMLHandler.getTagValue(stepnode, "bufferFlushMaxBytes"));
      maxRetries = Integer.valueOf(XMLHandler.getTagValue(stepnode, "maxRetries"));

      scanningFrequency = Long.valueOf(XMLHandler.getTagValue(stepnode,"scanningFrequency"));
      connectTimeout = Long.valueOf(XMLHandler.getTagValue(stepnode,"connectTimeout"));
      loadTimeout = Long.valueOf(XMLHandler.getTagValue(stepnode,"loadTimeout"));

      streamLoadProp = XMLHandler.getTagValue(stepnode, "streamLoadProp");
      mergeType = XMLHandler.getTagValue(stepnode, "mergeType");
      partialColumns = XMLHandler.getTagValue(stepnode, "partialColumns");
      deletable = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "deletable"));

      host = XMLHandler.getTagValue(stepnode, "host");
      port = XMLHandler.getTagValue(stepnode, "port");
      extConfig = XMLHandler.getTagValue(stepnode, "extConfig");
      databaseType = XMLHandler.getTagValue(stepnode, "databaseType");
      truncateTable = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "truncateTable"));;

      // Field data mapping
      int nrvalues = XMLHandler.countNodes(stepnode, "mapping");
      allocate(nrvalues);

      for (int i = 0; i < nrvalues; i++) {
        Node vnode = XMLHandler.getSubNodeByNr(stepnode, "mapping", i);

        fieldTable[i] = XMLHandler.getTagValue(vnode, "stream_name");
        fieldStream[i] = XMLHandler.getTagValue(vnode, "field_name");
        if (fieldStream[i] == null) {
          fieldStream[i] = fieldTable[i]; // default: the same name!
        }
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG,
          "DorisStreamLoaderMeta.Exception.UnableToReadStepInfoFromXML" ), e );
    }
  }

  public void setDefault() {
    fieldTable = null;
    fenodes = null;
    database = "";
    table = BaseMessages.getString(PKG, "DorisStreamLoaderMeta.DefaultTableName");
    username = "root";
    password = "";

    bufferFlushMaxRows = 10000;
    bufferFlushMaxBytes = 10 * 1024 * 1024;
    maxRetries = 3;
//    streamLoadProp = "format:json;read_json_by_line:true";
    streamLoadProp = "";
    deletable = false;

    partialColumns = "";
    mergeType = "";
    loadTimeout = 600;
    connectTimeout = 10;
    scanningFrequency = 50;

    host = "";
    port = "";
    databaseType = "";
    truncateTable = false;
    extConfig = "";

    allocate(0);
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    ").append(XMLHandler.addTagValue("fenodes", fenodes));
    retval.append("    ").append(XMLHandler.addTagValue("database", database));
    retval.append("    ").append(XMLHandler.addTagValue("table", table));
    retval.append("    ").append(XMLHandler.addTagValue("username", username));
    retval.append("    ").append(XMLHandler.addTagValue("password", password));
    retval.append("    ").append(XMLHandler.addTagValue("bufferFlushMaxRows", bufferFlushMaxRows));
    retval.append("    ").append(XMLHandler.addTagValue("bufferFlushMaxBytes", bufferFlushMaxBytes));
    retval.append("    ").append(XMLHandler.addTagValue("maxRetries", maxRetries));
    retval.append("    ").append(XMLHandler.addTagValue("streamLoadProp", streamLoadProp));
    retval.append("    ").append(XMLHandler.addTagValue("deletable", deletable));
    retval.append("    ").append(XMLHandler.addTagValue("scanningFrequency", scanningFrequency));
    retval.append("    ").append(XMLHandler.addTagValue("connectTimeout", connectTimeout));
    retval.append("    ").append(XMLHandler.addTagValue("loadTimeout", loadTimeout));
    retval.append("    ").append(XMLHandler.addTagValue("mergeType", mergeType));
    retval.append("    ").append(XMLHandler.addTagValue("partialColumns", partialColumns));
    retval.append("    ").append(XMLHandler.addTagValue("host", host));
    retval.append("    ").append(XMLHandler.addTagValue("port", port));
    retval.append("    ").append(XMLHandler.addTagValue("extConfig", extConfig));
    retval.append("    ").append(XMLHandler.addTagValue("databaseType", databaseType));
    retval.append("    ").append(XMLHandler.addTagValue("truncateTable", truncateTable));

    for (int i = 0; i < fieldTable.length; i++) {
      retval.append("      <mapping>").append(Const.CR);
      retval.append("        ").append(XMLHandler.addTagValue("stream_name", fieldTable[i]));
      retval.append("        ").append(XMLHandler.addTagValue("field_name", fieldStream[i]));
      retval.append("      </mapping>").append(Const.CR);
    }

    return retval.toString();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {

      // 初始化
      this.setDefault();

      fenodes = rep.getStepAttributeString(id_step, "fenodes");
      database = rep.getStepAttributeString(id_step, "database");
      table = rep.getStepAttributeString(id_step, "table");
      username = rep.getStepAttributeString(id_step, "username");
      password = rep.getStepAttributeString(id_step, "password");
      if (password == null) {
        password = "";
      }

      bufferFlushMaxRows = rep.getStepAttributeInteger(id_step, "bufferFlushMaxRows");
      bufferFlushMaxBytes = rep.getStepAttributeInteger(id_step, "bufferFlushMaxBytes");
      maxRetries = (int) rep.getStepAttributeInteger(id_step, "maxRetries");

      loadTimeout = rep.getStepAttributeInteger(id_step, "loadTimeout");
      scanningFrequency = rep.getStepAttributeInteger(id_step, "scanningFrequency");
      connectTimeout = rep.getStepAttributeInteger(id_step, "connectTimeout");

      mergeType = rep.getStepAttributeString(id_step, "mergeType");
      partialColumns = rep.getStepAttributeString(id_step, "partialColumns");

      streamLoadProp = rep.getStepAttributeString(id_step, "streamLoadProp");
      deletable = rep.getStepAttributeBoolean(id_step, "deletable");

      host = rep.getStepAttributeString(id_step, "host");
      port = rep.getStepAttributeString(id_step, "port");
      extConfig = rep.getStepAttributeString(id_step, "extConfig");
      databaseType = rep.getStepAttributeString(id_step, "databaseType");
      truncateTable = rep.getStepAttributeBoolean(id_step, "truncateTable");

      int nrvalues = rep.countNrStepAttributes(id_step, "stream_name");
      allocate(nrvalues);

      for (int i = 0; i < nrvalues; i++) {
        fieldTable[i] = rep.getStepAttributeString(id_step, i, "stream_name");
        fieldStream[i] = rep.getStepAttributeString(id_step, i, "field_name");
        if (fieldStream[i] == null) {
          fieldStream[i] = fieldTable[i];
        }
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "DorisStreamLoaderMeta.Exception.UnexpectedErrorReadingStepInfoFromRepository" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute(id_transformation, id_step, "fenodes", fenodes);
      rep.saveStepAttribute(id_transformation, id_step, "database", database);
      rep.saveStepAttribute(id_transformation, id_step, "table", table);
      rep.saveStepAttribute(id_transformation, id_step, "username", username);
      rep.saveStepAttribute(id_transformation, id_step, "password", password);
      rep.saveStepAttribute(id_transformation, id_step, "streamLoadProp", streamLoadProp);
      rep.saveStepAttribute(id_transformation, id_step, "bufferFlushMaxRows", bufferFlushMaxRows);
      rep.saveStepAttribute(id_transformation, id_step, "bufferFlushMaxBytes", bufferFlushMaxBytes);
      rep.saveStepAttribute(id_transformation, id_step, "maxRetries", maxRetries);
      rep.saveStepAttribute(id_transformation, id_step, "deletable", deletable);

      rep.saveStepAttribute(id_transformation, id_step, "mergeType", mergeType);
      rep.saveStepAttribute(id_transformation, id_step, "partialColumns", partialColumns);

      rep.saveStepAttribute(id_transformation, id_step, "loadTimeout", loadTimeout);
      rep.saveStepAttribute(id_transformation, id_step, "connectTimeout", connectTimeout);
      rep.saveStepAttribute(id_transformation, id_step, "scanningFrequency", scanningFrequency);

      rep.saveStepAttribute(id_transformation, id_step, "host", host);
      rep.saveStepAttribute(id_transformation, id_step, "port", port);
      rep.saveStepAttribute(id_transformation, id_step, "extConfig", extConfig);
      rep.saveStepAttribute(id_transformation, id_step, "databaseType", databaseType);
      rep.saveStepAttribute(id_transformation, id_step, "truncateTable", truncateTable);

      for (int i = 0; i < fieldTable.length; i++) {
        rep.saveStepAttribute(id_transformation, id_step, i, "stream_name", fieldTable[i]);
        rep.saveStepAttribute(id_transformation, id_step, i, "field_name", fieldStream[i]);
      }

    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "DorisStreamLoaderMeta.Exception.UnableToSaveStepInfoToRepository" )
          + id_step, e );
    }
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    // Default: nothing changes to rowMeta
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore ) {
    //todo: check parameters
  }


  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    return new DorisStreamLoader( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new DorisStreamLoaderData();
  }


  public String getFenodes() {
    return fenodes;
  }

  public void setFenodes(String fenodes) {
    this.fenodes = fenodes;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * key:value;key:value
   * @return
   */
  public String getStreamLoadProp() {
    return streamLoadProp;
  }

  public void setStreamLoadProp(String streamLoadProp) {
    this.streamLoadProp = streamLoadProp;
  }

  public long getBufferFlushMaxRows() {
    return bufferFlushMaxRows;
  }

  public void setBufferFlushMaxRows(long bufferFlushMaxRows) {
    this.bufferFlushMaxRows = bufferFlushMaxRows;
  }

  public long getBufferFlushMaxBytes() {
    return bufferFlushMaxBytes;
  }

  public void setBufferFlushMaxBytes(long bufferFlushMaxBytes) {
    this.bufferFlushMaxBytes = bufferFlushMaxBytes;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public boolean isDeletable() {
      return deletable;
  }

  public void setDeletable(boolean deletable) {
      this.deletable = deletable;
  }

  public String[] getFieldTable() {
    return fieldTable;
  }

  public void setFieldTable(String[] fieldTable) {
    this.fieldTable = fieldTable;
  }

  public String[] getFieldStream() {
    return fieldStream;
  }

  public void setFieldStream(String[] fieldStream) {
    this.fieldStream = fieldStream;
  }

  public long getScanningFrequency() {
    return scanningFrequency;
  }

  public void setScanningFrequency(long scanningFrequency) {
    this.scanningFrequency = scanningFrequency;
  }

  public long getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(long connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public long getLoadTimeout() {
    return loadTimeout;
  }

  public void setLoadTimeout(long loadTimeout) {
    this.loadTimeout = loadTimeout;
  }

  public String getMergeType() {
    return mergeType;
  }

  public void setMergeType(String mergeType) {
    this.mergeType = mergeType;
  }

  public String getPartialColumns() {
    return partialColumns;
  }

  public void setPartialColumns(String partialColumns) {
    this.partialColumns = partialColumns;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getDatabaseType() {
    return databaseType;
  }

  public void setDatabaseType(String databaseType) {
    this.databaseType = databaseType;
  }

  public Boolean getTruncateTable() {
    return truncateTable;
  }

  public void setTruncateTable(Boolean truncateTable) {
    this.truncateTable = truncateTable;
  }

  public String getExtConfig() {
    return extConfig;
  }

  public void setExtConfig(String extConfig) {
    this.extConfig = extConfig;
  }

  public void allocate(int nrvalues) {
    fieldTable = new String[nrvalues];
    fieldStream = new String[nrvalues];
  }

  @Override
  public String toString() {
    return "DorisStreamLoaderMeta{" +
            "fenodes='" + fenodes + '\'' +
            ", database='" + database + '\'' +
            ", table='" + table + '\'' +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", streamLoadProp='" + streamLoadProp + '\'' +
            ", bufferFlushMaxRows=" + bufferFlushMaxRows +
            ", bufferFlushMaxBytes=" + bufferFlushMaxBytes +
            ", maxRetries=" + maxRetries +
            ", deletable=" + deletable +
            ", fieldTable=" + Arrays.toString(fieldTable) +
            ", fieldStream=" + Arrays.toString(fieldStream) +
            ", scanningFrequency=" + scanningFrequency +
            ", connectTimeout=" + connectTimeout +
            ", loadTimeout=" + loadTimeout +
            ", mergeType='" + mergeType + '\'' +
            ", partialColumns='" + partialColumns + '\'' +
            '}';
  }

  /**
   * If we use injection we can have different arrays lengths.
   * We need synchronize them for consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
      int nrFields = (fieldTable == null) ? -1 : fieldTable.length;
      if (nrFields <= 0) {
          return;
      }
      String[][] rtnStrings = Utils.normalizeArrays(nrFields, fieldStream);
      fieldStream = rtnStrings[0];
  }
}
