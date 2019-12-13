package edu.stanford.futuredata.uniserve.utilities;

import com.google.protobuf.ByteString;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Utilities {
    public static String null_name = "__null__unready__";
    private static final Logger logger = LoggerFactory.getLogger(Utilities.class);

    public static Pair<String, Integer> parseConnectString(String connectString) {
        String[] hostPort = connectString.split(":");
        String host = hostPort[0];
        Integer port = Integer.parseInt(hostPort[1]);
        return new Pair<>(host, port);
    }

    public static ByteString objectToByteString(Serializable obj) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(obj);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Serialization Failed {} {}", obj, e);
            assert(false);
        }
        return ByteString.copyFrom(bos.toByteArray());
    }

    public static Object byteStringToObject(ByteString b) {
        ByteArrayInputStream bis = new ByteArrayInputStream(b.toByteArray());
        Object obj = null;
        try {
            ObjectInput in = new ObjectInputStream(bis);
            obj = in.readObject();
            in.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            logger.error("Deserialization Failed {} {}", b, e);
            assert(false);
        }
        return obj;
    }
}