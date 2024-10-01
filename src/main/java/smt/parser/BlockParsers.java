package smt.parser;

import com.prowidesoftware.swift.model.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BlockParsers {

  static void parseBlock1(Connection connection, SwiftBlock1 block1,
                                  int mid) throws SQLException {
    String query = "INSERT INTO block1(messageID, applicationId, serviceId, " +
            "logicalTerminal, sessionNumber, sequenceNumber) VALUES (?, ?, ?," +
            " ?, ?, ?)";

    PreparedStatement preparedStatement = connection.prepareStatement(query);

    preparedStatement.setInt(1, mid);
    preparedStatement.setString(2, block1.getApplicationId());
    preparedStatement.setString(3, block1.getServiceId());
    preparedStatement.setString(4, block1.getLogicalTerminal());
    preparedStatement.setString(5, block1.getSessionNumber());
    preparedStatement.setString(6, block1.getSequenceNumber());

    preparedStatement.executeUpdate();
  }

  static void parseInputBlock2(Connection connection,
                                       SwiftBlock2Input block2,
                                       int mid) throws SQLException {
    String query = "INSERT INTO block2_input(messageID, receiverAddress, " +
            "deliveryMonitoring, obsolescencePeriod, messagePriority, " +
            "messageType, direction) VALUES (?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement preparedStatement = connection.prepareStatement(query);

    preparedStatement.setInt(1, mid);
    preparedStatement.setString(2, block2.getReceiverAddress());
    preparedStatement.setString(3, block2.getDeliveryMonitoring());
    preparedStatement.setString(4, block2.getObsolescencePeriod());
    preparedStatement.setString(5, block2.getMessagePriority());
    preparedStatement.setString(6, block2.getMessageType());
    preparedStatement.setString(7, "I");

    preparedStatement.executeUpdate();
  }

  static void parseOutputBlock2(Connection connection,
                                        SwiftBlock2Output block2,
                                        int mid) throws SQLException {
    String query = "INSERT INTO block2_output(messageID, senderInputTime, " +
            "MIRDate, MIRLogicalTerminal, MIRSessionNumber, MIRSequenceNumber, " +
            "receiverOutputDate, receiverOutputTime, messagePriority, " +
            "messageType, direction) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement preparedStatement = connection.prepareStatement(query);

    preparedStatement.setInt(1, mid);
    preparedStatement.setString(2, block2.getSenderInputTime());
    preparedStatement.setString(3, block2.getMIRDate());
    preparedStatement.setString(4, block2.getMIRLogicalTerminal());
    preparedStatement.setString(5, block2.getMIRSessionNumber());
    preparedStatement.setString(6, block2.getMIRSequenceNumber());
    preparedStatement.setString(7, block2.getReceiverOutputDate());
    preparedStatement.setString(8, block2.getReceiverOutputTime());
    preparedStatement.setString(9, block2.getMessagePriority());
    preparedStatement.setString(10, block2.getMessageType());
    preparedStatement.setString(11, "O");

    preparedStatement.executeUpdate();
  }

  static void parseBlock3(Connection connection, SwiftBlock3 block3,
                           int mid) throws SQLException {
    String query = "INSERT INTO block3(messageID, t103, t113, t108, t119, " +
            "t423, t106, t424, t111, t121, t115, t165, t433, t434) VALUES " +
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement preparedStatement = connection.prepareStatement(query);

    preparedStatement.setInt(1, mid);
    preparedStatement.setString(2, block3.getTagValue("103"));
    preparedStatement.setString(3, block3.getTagValue("113"));
    preparedStatement.setString(4, block3.getTagValue("108"));
    preparedStatement.setString(5, block3.getTagValue("119"));
    preparedStatement.setString(6, block3.getTagValue("423"));
    preparedStatement.setString(7, block3.getTagValue("106"));
    preparedStatement.setString(8, block3.getTagValue("424"));
    preparedStatement.setString(9, block3.getTagValue("111"));
    preparedStatement.setString(10, block3.getTagValue("121"));
    preparedStatement.setString(11, block3.getTagValue("115"));
    preparedStatement.setString(12, block3.getTagValue("165"));
    preparedStatement.setString(13, block3.getTagValue("433"));
    preparedStatement.setString(14, block3.getTagValue("434"));

    preparedStatement.executeUpdate();
  }

  static void parseMT103Block4(Connection connection,
                                       SwiftBlock4 block4, int mid) throws SQLException {
    String query = "INSERT INTO mt103_block4(messageID, t20, t13C, t23B, " +
            "t23E, t26T, t32A, t33B, t36, t50A, t50F, t50K, t51A, t52A, t52D," +
            " t53A, t53B, t53D, t54A, t54B, t54D, t55A, t55B, t55D," +
            " t56A, t56C, t56D, t57A, t57B, t57C, t57D, t59, t59A, t59F, t70," +
            " t71A, t71F, t71G, t72, t77B) VALUES " +
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement preparedStatement = connection.prepareStatement(query);

    preparedStatement.setInt(1, mid);
    preparedStatement.setString(2, block4.getTagValue("20"));
    preparedStatement.setString(3, block4.getTagValue("13C"));
    preparedStatement.setString(4, block4.getTagValue("23B"));
    preparedStatement.setString(5, block4.getTagValue("23E"));
    preparedStatement.setString(6, block4.getTagValue("26T"));
    preparedStatement.setString(7, block4.getTagValue("32A"));
    preparedStatement.setString(8, block4.getTagValue("33B"));
    preparedStatement.setString(9, block4.getTagValue("36"));
    preparedStatement.setString(10, block4.getTagValue("50A"));
    preparedStatement.setString(11, block4.getTagValue("50F"));
    preparedStatement.setString(12, block4.getTagValue("50K"));
    preparedStatement.setString(13, block4.getTagValue("51A"));
    preparedStatement.setString(14, block4.getTagValue("52A"));
    preparedStatement.setString(15, block4.getTagValue("52D"));
    preparedStatement.setString(16, block4.getTagValue("53A"));
    preparedStatement.setString(17, block4.getTagValue("53B"));
    preparedStatement.setString(18, block4.getTagValue("53D"));
    preparedStatement.setString(19, block4.getTagValue("54A"));
    preparedStatement.setString(20, block4.getTagValue("54B"));
    preparedStatement.setString(21, block4.getTagValue("54D"));
    preparedStatement.setString(22, block4.getTagValue("55A"));
    preparedStatement.setString(23, block4.getTagValue("55B"));
    preparedStatement.setString(24, block4.getTagValue("55D"));
    preparedStatement.setString(25, block4.getTagValue("56A"));
    preparedStatement.setString(26, block4.getTagValue("56C"));
    preparedStatement.setString(27, block4.getTagValue("56D"));
    preparedStatement.setString(28, block4.getTagValue("57A"));
    preparedStatement.setString(29, block4.getTagValue("57B"));
    preparedStatement.setString(30, block4.getTagValue("57C"));
    preparedStatement.setString(31, block4.getTagValue("57D"));
    preparedStatement.setString(32, block4.getTagValue("59"));
    preparedStatement.setString(33, block4.getTagValue("59A"));
    preparedStatement.setString(34, block4.getTagValue("59F"));
    preparedStatement.setString(35, block4.getTagValue("70"));
    preparedStatement.setString(36, block4.getTagValue("71A"));
    preparedStatement.setString(37, block4.getTagValue("71F"));
    preparedStatement.setString(38, block4.getTagValue("71G"));
    preparedStatement.setString(39, block4.getTagValue("72"));
    preparedStatement.setString(40, block4.getTagValue("77B"));

    preparedStatement.executeUpdate();
  }

  static void parseMT202Block4(Connection connection, SwiftBlock4 block4,
                               int mid) throws SQLException {
    String query = "INSERT INTO mt202_block4(messageID, t20, t21, t13C, t32A," +
            " t52A, t52D, t53A, t53B, t53D, t54A, t54B, t54D, t56A, t56D, " +
            "t57A, t57B, t57D, t58A, t58D, t72) VALUES (?, ?, ?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement preparedStatement = connection.prepareStatement(query);
    preparedStatement.setInt(1, mid);
    preparedStatement.setString(2, block4.getTagValue("20"));
    preparedStatement.setString(3, block4.getTagValue("21"));
    preparedStatement.setString(4, block4.getTagValue("13C"));
    preparedStatement.setString(5, block4.getTagValue("32A"));
    preparedStatement.setString(6, block4.getTagValue("52A"));
    preparedStatement.setString(7, block4.getTagValue("52D"));
    preparedStatement.setString(8, block4.getTagValue("53A"));
    preparedStatement.setString(9, block4.getTagValue("53B"));
    preparedStatement.setString(10, block4.getTagValue("53D"));
    preparedStatement.setString(11, block4.getTagValue("54A"));
    preparedStatement.setString(12, block4.getTagValue("54B"));
    preparedStatement.setString(13, block4.getTagValue("54D"));
    preparedStatement.setString(14, block4.getTagValue("56A"));;
    preparedStatement.setString(15, block4.getTagValue("56D"));
    preparedStatement.setString(16, block4.getTagValue("57A"));
    preparedStatement.setString(17, block4.getTagValue("57B"));
    preparedStatement.setString(18, block4.getTagValue("57D"));
    preparedStatement.setString(19, block4.getTagValue("58A"));
    preparedStatement.setString(20, block4.getTagValue("58D"));
    preparedStatement.setString(21, block4.getTagValue("72"));

    preparedStatement.executeUpdate();
  }

  static void parseBlock5(Connection connection, SwiftBlock5 block5, int mid) throws SQLException {
    String query = "INSERT INTO block5(messageID, MAC, PAC, CHK, TNG, PDE, " +
            "SYS, PDM, DLM, MRF) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement preparedStatement = connection.prepareStatement(query);

    preparedStatement.setInt(1, mid);
    preparedStatement.setString(2, block5.getTagValue("MAC"));
    preparedStatement.setString(3, block5.getTagValue("PAC"));
    preparedStatement.setString(4, block5.getTagValue("CHK"));
    preparedStatement.setString(5, block5.getTagValue("TNG"));
    preparedStatement.setString(6, block5.getTagValue("PDE"));
    preparedStatement.setString(7, block5.getTagValue("SYS"));
    preparedStatement.setString(8, block5.getTagValue("PDM"));
    preparedStatement.setString(9, block5.getTagValue("DLM"));
    preparedStatement.setString(10, block5.getTagValue("MRF"));

    preparedStatement.executeUpdate();
  }

}
