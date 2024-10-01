package smx.parser;

import com.prowidesoftware.swift.model.mx.MxPacs00800107;
import com.prowidesoftware.swift.utils.Lib;

import java.io.IOException;

/**
 * Reads an MX from file and prints its default conversion to JSON
 */
public class MxToJsonExample {

    public static void main(String[] args) throws IOException {
        MxPacs00800107 mx = MxPacs00800107.parse(Lib.readResource("pacs.008.001.07.xml"));

        String json = mx.toJson();

        System.out.println(json);

        MxPacs00800107 mx2 = MxPacs00800107.fromJson(json);

        System.out.println(mx2.message());
    }
}
