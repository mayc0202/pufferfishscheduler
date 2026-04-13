package com.pufferfishscheduler.plugin;

import com.pufferfishscheduler.plugin.serializer.DorisRecordSerializer;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * DorisStreamLoaderData
 */
public class DorisStreamLoaderData extends BaseStepData implements StepDataInterface {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...
  public ValueMetaInterface[] formatMeta;
  public String[] fieldNames;

  public DorisRecordSerializer serializer;
  /**
   * Default constructor.
   */
  public DorisStreamLoaderData() {
    super();

    db = null;
  }
}
