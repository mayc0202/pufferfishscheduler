package com.pufferfishscheduler.worker.quartz.constants;

/**
 * 常量
 *
 * @author Mayc
 * @since 2025-07-30  13:04
 */
public class WorkerConstants {
    public static final String TASK_CLASS_NAME = "TASK_";
    public static final String TASK_PROPERTIES = "JOB_PROPERTIES";

    public static final String MISFIRE_DEFAULT = "0";
    public static final String MISFIRE_IGNORE_MISFIRES = "1";
    public static final String MISFIRE_FIRE_AND_PROCEED = "2";
    public static final String MISFIRE_DO_NOTHING = "3";

    public enum Status {
        NORMAL("0"), PAUSE("1");

        private final String value;

        Status(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
