package dev.bigspark.lib.ibmmq;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum ServiceErrors implements ErrorCode {
    SERVICE_ERROR_001("Cannot parse record from message '{}': {}");

    private final String msg;

    private ServiceErrors(String msg) {
        this.msg = msg;
    }

    public String getCode() {
        return this.name();
    }

    public String getMessage() {
        return this.msg;
    }
}
