package edu.stanford.futuredata.uniserve.utilities;

import org.javatuples.Pair;

public class Utilities {
    public static Pair<String, Integer> parseConnectString(String connectString) {
        String[] hostPort = connectString.split(":");
        String host = hostPort[0];
        Integer port = Integer.parseInt(hostPort[1]);
        return new Pair<>(host, port);
    }
}
