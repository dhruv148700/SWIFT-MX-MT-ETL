package smx.parser;

import com.prowidesoftware.swift.model.mx.BusinessAppHdrV01;
import com.prowidesoftware.swift.model.mx.MxPacs00800107;
import com.prowidesoftware.swift.utils.Lib;

import java.io.IOException;

/**
 * Reads content from the message header, using the specific header implementation model
 *
 * <p>Running this will produce:<br>
 * <pre>
 * Header from: ABCDEFGH
 * </pre>
 */
public class MxParseMessageWithHeader2 {

    public static void main(String[] args) throws IOException {

        // parse the XML message content from a resource file
        MxPacs00800107 mx = MxPacs00800107.parse(Lib.readResource("pacs.008.001.07_ClrSysMmbId.xml"));

        // you can further cast this to a specific AppHdr implementation to read more data
        BusinessAppHdrV01 header = (BusinessAppHdrV01) mx.getAppHdr();
        System.out.println("Header from: " + header.getFr().getFIId().getFinInstnId().getClrSysMmbId().getMmbId());
    }

}
