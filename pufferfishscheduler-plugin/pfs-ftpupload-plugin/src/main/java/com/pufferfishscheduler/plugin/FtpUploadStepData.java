package com.pufferfishscheduler.plugin;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class FtpUploadStepData extends BaseStepData implements StepDataInterface {

	public RowMetaInterface outputRowMeta;
	
	public FtpUploadStepData() {
		super();
	}
	
}
