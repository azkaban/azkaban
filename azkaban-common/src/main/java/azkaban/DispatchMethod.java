package azkaban;

import azkaban.utils.Props;

public enum DispatchMethod {
    PUSH,
    POLL,
    CONTAINERIZED;

    public static DispatchMethod getDispatchModel(Props azkabanProps) {
        return DispatchMethod.valueOf(
                azkabanProps.getString(Constants.ConfigurationKeys.AZKABAN_DISPATCH_MODEL, "PUSH").toUpperCase());
    }
}
