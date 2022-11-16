package org.neo4j.tool.util;

public class Print {
    public static void println(String fmt, Object... args) {
        System.out.printf(fmt + "%n", args);
    }

    public static void printf(String fmt, Object... args) {
        System.out.printf(fmt, args);
    }
}
