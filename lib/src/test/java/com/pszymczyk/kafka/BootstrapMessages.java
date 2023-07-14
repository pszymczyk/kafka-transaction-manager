package com.pszymczyk.kafka;

import java.util.List;

public class BootstrapMessages {

    static List<String> getBootstrapMessages() {
        return List.of(
            "input:ping"
        );
    }
}
