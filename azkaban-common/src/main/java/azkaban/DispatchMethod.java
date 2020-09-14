package azkaban;

import azkaban.utils.Props;

public enum DispatchMethod {
    PUSH("push"),
    POLL("poll"),
    CONTAINERIZED("containerized");
    private final String model;

    private DispatchMethod(String model) {
        this.model = model;
    }

    public static DispatchMethod getDispatchModel(Props azkabanProps) {
        return DispatchMethod.valueOf(
                azkabanProps.getString(Constants.ConfigurationKeys.AZKABAN_DISPATCH_MODEL, "push").toUpperCase());
    }
}
