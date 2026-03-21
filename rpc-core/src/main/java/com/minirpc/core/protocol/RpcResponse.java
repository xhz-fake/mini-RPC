package com.minirpc.core.protocol;

import java.io.Serializable;

public class RpcResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private String requestId;
    private Object data;
    private String error;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean hasError() {
        return error != null && !error.isBlank();
    }
}
