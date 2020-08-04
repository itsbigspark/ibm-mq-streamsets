package dev.bigspark.lib.ibmmq;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum BasicErrors implements ErrorCode {
    BASIC_01("Batch size '{}' cannot be less than 1"),
    BASIC_02("Batch wait time '{}' cannot be less than 1");

    private final String msg;

    private BasicErrors(String msg) {
        this.msg = msg;
    }

    public String getCode() {
        return this.name();
    }

    public String getMessage() {
        return this.msg;
    }
}
