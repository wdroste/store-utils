package org.neo4j.tool.dto;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class IndexStatus {

    public enum State {
        ONLINE, POPULATING, FAILED, OTHER;

        public boolean isFailed() {
            return FAILED == this || OTHER == this;
        }

        public boolean isOk() {
            return ONLINE == this || POPULATING == this;
        }

        public boolean isOnline() {
            return ONLINE == this;
        }
    }

    State state;
    float progress;
}
