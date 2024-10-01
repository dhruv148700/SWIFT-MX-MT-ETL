package smx.parser;

import com.prowidesoftware.swift.model.mx.MxPacs00800107;
import com.prowidesoftware.swift.model.mx.dic.ActiveCurrencyAndAmount;
import com.prowidesoftware.swift.utils.Lib;

import java.io.IOException;

/**
 * Reads content from the message header, using the generic header implementation model
 *
 * <p>Running this will produce:<br>
 *
 * <pre>
 * Header from: ABCDUS33XXX
 * Header to: EFGHUS33XXX
 * Header reference: 12312312312
 * Amount: USD 5000
 * </pre>
 */
public class MxParseMessageWithHeader {

    public static void main(String[] args) throws IOException {

        // parse the XML message content from a resource file
        MxPacs00800107 mx = MxPacs00800107.parse(Lib.readResource("pacs.008.001.07.xml"));
        System.out.println(mx.toString());

        // access message header data from the java model
        System.out.println("Header from: " + mx.getAppHdr().from());
        System.out.println("Header to: " + mx.getAppHdr().to());
        System.out.println("Header reference: " + mx.getAppHdr().reference());

        // notice the from/to methods in the generic model will only return values when the header BIC option is
        // present. For other structure options such as reading a ClrSysMmbId you can further cast this to a specific
        // AppHdr implementation. The AppHdr is just an interface.

        // access message document data from the java model
        ActiveCurrencyAndAmount amount = mx.getFIToFICstmrCdtTrf().getCdtTrfTxInf().get(0).getIntrBkSttlmAmt();
        System.out.println("Amount: " + amount.getCcy() + " " + amount.getValue());
    }

}
