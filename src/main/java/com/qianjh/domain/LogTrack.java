package com.qianjh.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author qianjh
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class LogTrack {
    private String appid;
    private String type;
    private Long sendTime;
    private Long eventTime;
    private Long receiveTime;
}
