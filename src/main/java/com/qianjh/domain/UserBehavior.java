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
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Long categoryId;
    private String behavior;
    private Long timestamp;
}
