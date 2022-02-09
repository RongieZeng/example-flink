package io.github.streamingwithflink.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class LogEvent {

    private String eventType;

    private Integer userId;

    private Integer orgId;

    private Integer parentOrgId;

    private Long timestamp;
}