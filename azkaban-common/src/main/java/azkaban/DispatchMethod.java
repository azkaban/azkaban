package azkaban;

import azkaban.utils.Props;

public enum DispatchMethod {
    PUSH,
    POLL,
    CONTAINERIZED;

    public static DispatchMethod getDispatchModel(Props azkabanProps) {
        return DispatchMethod.valueOf(
                azkabanProps.getString(Constants.ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD, "PUSH").toUpperCase());
    }
}
