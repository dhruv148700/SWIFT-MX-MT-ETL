package smx.parser;

import com.prowidesoftware.swift.model.mx.MxPacs00800107;
import com.prowidesoftware.swift.utils.Lib;
import com.prowidesoftware.swift.model.mx.dic.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Properties;

import static smx.parser.MessageElementParsers.*;

public class Pacs00800107Parser {

   protected static Connection connection = null;

  /**
   * Connect to database storing Pacs00800107 messages
   * @return Database Connection
   */
  private static void setUpConnection() {
    Properties props = new Properties();

    try (FileInputStream input =
                 new FileInputStream("config/properties")) {
      props.load(input);
    } catch (IOException e) {
      e.printStackTrace();
    }

    final String URL = props.getProperty("MX_URL");
    final String USER = props.getProperty("RDS_USER");
    final String PASSWORD = props.getProperty("RDS_PASSWORD");

    try {
      connection = DriverManager.getConnection(URL, USER, PASSWORD);
      System.out.println("Connection established successfully.");
    } catch (SQLException e) {
      e.printStackTrace();
      System.out.println("Failed to establish connection.");
    }
  }

  /**
   * Close connection to database storing Pacs00800107 messages
   */
  private static void closeConnection() {
    if (connection != null) {
      try {
        connection.close();
        System.out.println("Connection closed successfully.");
      } catch (SQLException e) {
        e.printStackTrace();
        System.out.println("Failed to close connection.");
      }
    }
  }

  public static void main(String[] args) throws IOException, SQLException {

    // Read input pacs00800107 xml file
    MxPacs00800107 mx = MxPacs00800107.parse(Lib.readResource("pacs.008.001.07.xml"));

    // Initiate database connection
    setUpConnection();

    // Access FIToFICustomerCreditTransfer element
    FIToFICustomerCreditTransferV07 fiToFICustomerCreditTransfer07 =
            mx.getFIToFICstmrCdtTrf();

    // Access GroupHeader element
    GroupHeader70 grpHdr = fiToFICustomerCreditTransfer07.getGrpHdr();

    // Store the group header
    parseGroupHeader(grpHdr);

    // Access the list of CreditTransferTransaction30 elements
    List<CreditTransferTransaction30> cdtTrfTxInf = fiToFICustomerCreditTransfer07.getCdtTrfTxInf();

//    for (CreditTransferTransaction30 creditTransferTransaction : cdtTrfTxInf) {
//      parseCreditTransferTransaction(creditTransferTransaction);
//    }

    // Close database connection
    closeConnection();
  }

  private static void parseCreditTransferTransaction(CreditTransferTransaction30 crdTrnsfrTx) throws SQLException {

    // Payment identification
    PaymentIdentification3 pmtId = crdTrnsfrTx.getPmtId();
    parsePaymentIdentification(pmtId);

    // Payment type information
    PaymentTypeInformation21 pmtTpInf = crdTrnsfrTx.getPmtTpInf();
    parsePaymentTypeInformation(pmtTpInf);

    // Interbank settlement amount
    ActiveCurrencyAndAmount intrBkSttlmAmt = crdTrnsfrTx.getIntrBkSttlmAmt();
    parseActiveCurrencyAndAmount(intrBkSttlmAmt);

    // Interbank settlement date
    LocalDate intrBkSttlmDt = crdTrnsfrTx.getIntrBkSttlmDt();

    // Settlement priority
    Priority3Code sttlmPrty = crdTrnsfrTx.getSttlmPrty();

    // Settlement time indication
    SettlementDateTimeIndication1 sttlmTmIndctn = crdTrnsfrTx.getSttlmTmIndctn();
    parseSettlementDateTime(sttlmTmIndctn);

    // Settlement time request
    SettlementTimeRequest2 sttlmTmReq = crdTrnsfrTx.getSttlmTmReq();
    parseSettlementTimeRequest(sttlmTmReq);

    // Acceptance date and time
    OffsetDateTime accptncDtTm = crdTrnsfrTx.getAccptncDtTm();

    // Pooling adjustment date
    LocalDate poolgAdjstmntDt = crdTrnsfrTx.getPoolgAdjstmntDt();

    // Instructed Amount
    ActiveOrHistoricCurrencyAndAmount instdAmt = crdTrnsfrTx.getInstdAmt();
    parseActiveOrHistoricCurrencyAmount(instdAmt);

    // Exchange Rate
    BigDecimal xchgRate = crdTrnsfrTx.getXchgRate();

    // Charge Bearer
    ChargeBearerType1Code chrgBr = crdTrnsfrTx.getChrgBr();

    // Previous instructing agent 1
    BranchAndFinancialInstitutionIdentification5 prvsInstgAgt1 = crdTrnsfrTx.getPrvsInstgAgt1();
    parseBranchAndFinancialInstitutionIdentification(prvsInstgAgt1);

    // Previous instructing agent 1 account
    CashAccount24 prvsInstgAgt1Acct = crdTrnsfrTx.getPrvsInstgAgt1Acct();
    parseCashAccount(prvsInstgAgt1Acct);

    // Previous instructing agent 2
    BranchAndFinancialInstitutionIdentification5 prvsInstgAgt2 = crdTrnsfrTx.getPrvsInstgAgt2();
    parseBranchAndFinancialInstitutionIdentification(prvsInstgAgt2);

    // Previous instructing agent 2 account
    CashAccount24 prvsInstgAgt2Acct = crdTrnsfrTx.getPrvsInstgAgt2Acct();
    parseCashAccount(prvsInstgAgt2Acct);

    // Previous instructing agent 3
    BranchAndFinancialInstitutionIdentification5 prvsInstgAgt3 = crdTrnsfrTx.getPrvsInstgAgt3();
    parseBranchAndFinancialInstitutionIdentification(prvsInstgAgt3);

    // Previous instructing agent 3 account
    CashAccount24 prvsInstgAgt3Acct = crdTrnsfrTx.getPrvsInstgAgt3Acct();
    parseCashAccount(prvsInstgAgt3Acct);

    // Instructing Agent
    BranchAndFinancialInstitutionIdentification5 instgAgt = crdTrnsfrTx.getInstgAgt();
    parseBranchAndFinancialInstitutionIdentification(instgAgt);

    // Instructed Agent
    BranchAndFinancialInstitutionIdentification5 instdAgt = crdTrnsfrTx.getInstdAgt();
    parseBranchAndFinancialInstitutionIdentification(instdAgt);

    // Intermediary Agent 1
    BranchAndFinancialInstitutionIdentification5 intrmyAgt1 = crdTrnsfrTx.getIntrmyAgt1();
    parseBranchAndFinancialInstitutionIdentification(intrmyAgt1);

    // Intermediary Agent 1 Account
    CashAccount24 intrmyAgt1Acct = crdTrnsfrTx.getIntrmyAgt1Acct();
    parseCashAccount(intrmyAgt1Acct);

    // Intermediary Agent 2
    BranchAndFinancialInstitutionIdentification5 intrmyAgt2 = crdTrnsfrTx.getIntrmyAgt2();
    parseBranchAndFinancialInstitutionIdentification(intrmyAgt2);

    // Intermediary Agent 2 Account
    CashAccount24 intrmyAgt2Acct = crdTrnsfrTx.getIntrmyAgt2Acct();
    parseCashAccount(intrmyAgt2Acct);

    // Intermediary Agent 3
    BranchAndFinancialInstitutionIdentification5 intrmyAgt3 = crdTrnsfrTx.getIntrmyAgt3();
    parseBranchAndFinancialInstitutionIdentification(intrmyAgt3);

    // Intermediary Agent 3 Account
    CashAccount24 intrmyAgt3Acct = crdTrnsfrTx.getIntrmyAgt3Acct();
    parseCashAccount(intrmyAgt3Acct);

    // Ultimate Debtor
    PartyIdentification125 ultmtDbtr = crdTrnsfrTx.getUltmtDbtr();
    System.out.print("Ultimate Debtor: ");
    parsePartyIdentification(ultmtDbtr);

    // Initiating Party
    PartyIdentification125 initgPty = crdTrnsfrTx.getInitgPty();
    System.out.print("Initiating Party: ");
    parsePartyIdentification(initgPty);

    // Debtor
    PartyIdentification125 dbtr = crdTrnsfrTx.getDbtr();
    System.out.print("Debtor: ");
    parsePartyIdentification(dbtr);

    // Debtor Account
    CashAccount24 dbtrAcct = crdTrnsfrTx.getDbtrAcct();
    System.out.print("Debtor Account: ");
    parseCashAccount(dbtrAcct);

    // Debtor Agent
    BranchAndFinancialInstitutionIdentification5 dbtrAgt = crdTrnsfrTx.getDbtrAgt();
    System.out.println("Debtor Agent: ");
    parseBranchAndFinancialInstitutionIdentification(dbtrAgt);

    // Debtor Agent Account
    CashAccount24 dbtrAgtAcct = crdTrnsfrTx.getDbtrAgtAcct();
    System.out.print("Debtor Agent Account: ");
    parseCashAccount(dbtrAgtAcct);

    // Creditor Agent
    BranchAndFinancialInstitutionIdentification5 cdtrAgt = crdTrnsfrTx.getCdtrAgt();
    System.out.println("Creditor Agent: ");
    parseBranchAndFinancialInstitutionIdentification(cdtrAgt);

    // Creditor Agent Account
    CashAccount24 cdtrAgtAcct = crdTrnsfrTx.getCdtrAgtAcct();
    System.out.print("Creditor Agent Account: ");
    parseCashAccount(cdtrAgtAcct);

    // Creditor
    PartyIdentification125 cdtr = crdTrnsfrTx.getCdtr();
    System.out.print("Creditor: ");
    parsePartyIdentification(cdtr);

    // Creditor Account
    CashAccount24 cdtrAcct = crdTrnsfrTx.getCdtrAcct();
    System.out.print("Creditor Account: ");
    parseCashAccount(cdtrAcct);

    // Ultimate Creditor
    PartyIdentification125 ultmtCdtr = crdTrnsfrTx.getUltmtCdtr();
    System.out.print("Ultimate Creditor: ");
    parsePartyIdentification(ultmtCdtr);

    // Instructions for Creditor Agent
    System.out.println("Instructions for Creditor Agent: ");
    for (InstructionForCreditorAgent1 i : crdTrnsfrTx.getInstrForCdtrAgt()) {
      // Code
      Instruction3Code cd = i.getCd();
      System.out.println("Code: " + cd);

      // Instruction Information
      String instrInf = i.getInstrInf();
      System.out.println("Instruction Information: " + instrInf);
    }

    // Instructions for Next Agent
    System.out.println("Instructions for Next Agent: ");
    for (InstructionForNextAgent1 i : crdTrnsfrTx.getInstrForNxtAgt()) {
      // Code
      Instruction4Code cd = i.getCd();
      System.out.println("Code: " + cd);

      // Instruction Information
      String instrInf = i.getInstrInf();
      System.out.println("Instruction Information: " + instrInf);
    }

    // Purpose
    Purpose2Choice purp = crdTrnsfrTx.getPurp();
    System.out.println("Purpose: " + purp);

    // Regulatory Reporting
    for (RegulatoryReporting3 r : crdTrnsfrTx.getRgltryRptg()) {
      // Debit-Credit Reporting Indicator
      RegulatoryReportingType1Code dbtCdtRptgInd = r.getDbtCdtRptgInd();
      System.out.println("Debit-Credit Reporting Indicator: " + dbtCdtRptgInd);

      // Authority
      RegulatoryAuthority2 authrty = r.getAuthrty();
      System.out.print("Authority: ");
      parseRegulatoryAuthority(authrty);

      for (StructuredRegulatoryReporting3 dtl : r.getDtls()) {
        // Type
        String tp = dtl.getTp();
        System.out.println("Type: " + tp);

        // Date
        LocalDate dt = dtl.getDt();
        System.out.println("Date: " + dt);

        // Country
        String ctry = dtl.getCtry();
        System.out.println("Country: " + ctry);

        // Code
        String cd = dtl.getCd();
        System.out.println("Code: " + cd);

        // Amount
        ActiveOrHistoricCurrencyAndAmount amt = dtl.getAmt();
        System.out.println("Amount: ");
        parseActiveOrHistoricCurrencyAmount(amt);

        // Information
        System.out.println("Information: ");
        for (String s : dtl.getInf()) {
          System.out.println(s);
        }
      }

    }

    // Tax
    TaxInformation6 tax = crdTrnsfrTx.getTax();
    System.out.print("Tax: ");
    parseTaxInformation(tax);

    // Related Remittance Information
    System.out.println("Related Remittance Information: ");
    for (RemittanceLocation4 rl : crdTrnsfrTx.getRltdRmtInf()) {
      // Remittance ID
      String rmtId = rl.getRmtId();
      System.out.println("Remittance ID: " + rmtId);

      // Remittance Location Details
      System.out.println("Remittance Location Details: ");
      for (RemittanceLocationDetails1 rld : rl.getRmtLctnDtls()) {

        // Method
        RemittanceLocationMethod2Code mtd = rld.getMtd();
        System.out.println("Method: " + mtd);

        // Electronic Address
        String elctrncAdr = rld.getElctrncAdr();
        System.out.println("Electronic Address: " + elctrncAdr);

        // Postal Address
        NameAndAddress10 pstlAdr = rld.getPstlAdr();
        System.out.println("Postal Address: ");
        if (pstlAdr != null) {
          // Name
          String nm = pstlAdr.getNm();
          System.out.println("Name: " + nm);

          // Address
          PostalAddress6 adr = pstlAdr.getAdr();
          System.out.print("Address: ");
          parsePostalAddress(adr);
        } else {
          System.out.println("null");
        }
      }
    }

    // Remittance Information
    RemittanceInformation15 rmtInf = crdTrnsfrTx.getRmtInf();
    System.out.println("Remittance Information: ");
    if (rmtInf != null) {
      for (String s : rmtInf.getUstrd()) {
        System.out.println(s);
      }
      for (StructuredRemittanceInformation15 sri : rmtInf.getStrd()) {
        // TODO
        System.out.println();
      }
    } else {
      System.out.println("null");
    }

    // Supplementary Data
    System.out.println("Supplementary Data: ");
    for (SupplementaryData1 sd : crdTrnsfrTx.getSplmtryData()) {
      // Place and Name
      String plcAndNm = sd.getPlcAndNm();
      System.out.println("Place and Name: " + plcAndNm);

      // Envelope
      SupplementaryDataEnvelope1 envlp = sd.getEnvlp();
    }


  }

  private static void parseTaxInformation(TaxInformation6 tax) {
    if (tax != null) {
      // Creditor
      TaxParty1 cdtr = tax.getCdtr();
      System.out.print("\nCreditor: ");
      if (cdtr != null) {
        // Tax Identification
        String taxId = cdtr.getTaxId();
        System.out.println("\nTax Identification: " + taxId);

        // Registration Identification
        String regnId = cdtr.getRegnId();
        System.out.println("Registration Identification: " + regnId);

        // Tax Type
        String taxTp = cdtr.getTaxTp();
        System.out.println("Tax Type: " + taxTp);
      } else {
        System.out.println("null");
      }

      // Debtor
      TaxParty2 dbtr = tax.getDbtr();
      System.out.print("Debtor: ");
      if (dbtr != null) {
        // Tax Identification
        String taxId = dbtr.getTaxId();
        System.out.println("\nTax Identification: " + taxId);

        // Registration Identification
        String regnId = dbtr.getRegnId();
        System.out.println("Registration Identification: " + regnId);

        // Tax Type
        String taxTp = dbtr.getTaxTp();
        System.out.println("Tax Type: " + taxTp);

        // Authorisation
        TaxAuthorisation1 authstn = dbtr.getAuthstn();
        System.out.print("Authorisation: ");
        if (authstn != null) {
          // Title
          String titl = authstn.getTitl();
          System.out.println("\nTitle: " + titl);

          // Name
          String nm = authstn.getNm();
          System.out.println("Name: " + nm);
        } else {
          System.out.println("null");
        }
      } else {
        System.out.println("null");
      }

      // Administration Zone
      String admstnZn = tax.getAdmstnZn();
      System.out.println("Administration Zone: " + admstnZn);

      // Reference Number
      String refNb = tax.getRefNb();
      System.out.println("Reference Number: " + refNb);

      // Method
      String mtd = tax.getMtd();
      System.out.println("Method: " + mtd);

      // Total Taxable Base Amount
      ActiveOrHistoricCurrencyAndAmount ttlTaxblBaseAmt = tax.getTtlTaxblBaseAmt();
      System.out.print("Total Taxable Base Amount: ");
      parseActiveOrHistoricCurrencyAmount(ttlTaxblBaseAmt);

      // Total Tax Amount
      ActiveOrHistoricCurrencyAndAmount ttlTaxAmt = tax.getTtlTaxAmt();
      System.out.print("Total Tax Amount: ");
      parseActiveOrHistoricCurrencyAmount(ttlTaxAmt);

      // Date
      LocalDate dt = tax.getDt();
      System.out.println("Date: " + dt);

      // Sequence Number
      BigDecimal seqNb = tax.getSeqNb();
      System.out.println("Sequence Number: " + seqNb);

      // Records
      for (TaxRecord2 tr : tax.getRcrd()) {
        // Type
        String tp = tr.getTp();
        System.out.println("Type: " + tp);

        // Category
        String ctgy = tr.getCtgy();
        System.out.println("Category: " + ctgy);

        // Category Details
        String ctgyDtls = tr.getCtgyDtls();
        System.out.println("Category Details: " + ctgyDtls);

        // Debtor Status
        String dbtrSts = tr.getDbtrSts();
        System.out.println("Debtor Status: " + dbtrSts);

        // Certification ID
        String certId = tr.getCertId();
        System.out.println("Certification ID: " + certId);

        // Forms Code
        String frmsCd = tr.getFrmsCd();
        System.out.println("Forms Code: " + frmsCd);

        // Period
        TaxPeriod2 prd = tr.getPrd();
        System.out.print("Period: ");
        parseTaxPeriod(prd);

        // Tax Amount
        TaxAmount2 taxAmt = tr.getTaxAmt();
        System.out.print("Tax Amount: ");
        parseTaxAmount(taxAmt);

        // Additional Information
        String addtlInf = tr.getAddtlInf();
      }
    } else {
      System.out.println("null");
    }
  }

  private static void parseTaxAmount(TaxAmount2 taxAmt) {
    if (taxAmt != null) {
      // Rate
      BigDecimal rate = taxAmt.getRate();
      System.out.println("\nRate: " + rate);

      // Taxable Base Amount
      ActiveOrHistoricCurrencyAndAmount taxblBaseAmt = taxAmt.getTaxblBaseAmt();
      System.out.print("Taxable Base Amount: ");
      parseActiveOrHistoricCurrencyAmount(taxblBaseAmt);

      // Total Amount
      ActiveOrHistoricCurrencyAndAmount ttlAmt = taxAmt.getTtlAmt();
      System.out.print("Total Amount: ");
      parseActiveOrHistoricCurrencyAmount(ttlAmt);

      // Details
      for (TaxRecordDetails2 dtl : taxAmt.getDtls()) {
        // Period
        TaxPeriod2 prd1 = dtl.getPrd();
        System.out.print("Period: ");
        parseTaxPeriod(prd1);

        // Amount
        ActiveOrHistoricCurrencyAndAmount amt = dtl.getAmt();
        System.out.print("Amount: ");
        parseActiveOrHistoricCurrencyAmount(amt);
      }
    } else {
      System.out.println("null");
    }
  }

  private static void parseTaxPeriod(TaxPeriod2 prd) {
    if (prd != null) {
      // Year
      LocalDate yr = prd.getYr();
      System.out.println("Year: " + yr);

      // Type
      TaxRecordPeriod1Code tp1 = prd.getTp();
      System.out.println("Type: " + tp1);

      // From To Date
      DatePeriod2 frToDt = prd.getFrToDt();

      // From Date
      LocalDate frDt = frToDt.getFrDt();
      System.out.println("From Date: " + frDt);

      // To Date
      LocalDate toDt = frToDt.getToDt();
      System.out.println("To Date: " + toDt);
    } else {
      System.out.println("null");
    }
  }

  private static void parseRegulatoryAuthority(RegulatoryAuthority2 authrty) {
    if (authrty == null) {
      System.out.println("null");
    } else {
      // Name
      String nm = authrty.getNm();
      System.out.println("Name: " + nm);

      // Country
      String ctry = authrty.getCtry();
      System.out.println("Country: " + ctry);
    }
  }

  /**
   * Load the group header information into GroupHeader Table
   * @param grpHdr - GroupHeader70 element (Non null)
   */
  private static int parseGroupHeader(GroupHeader70 grpHdr) throws SQLException {
    String query = "INSERT INTO GroupHeader (MsgId, CreDtTm, BtchBookg, " +
            "NbOfTxs, CtrlSum, TtlIntrBkSttlmAmt, IntrBkSttlmDt, SttlmInf, " +
            "PmtTpInf, InstgAgt, InstdAgt) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    // Message ID
    stmt.setString(1, grpHdr.getMsgId());

    // Creation data and time
    stmt.setString(2, grpHdr.getCreDtTm().toString());

    // Batch booking
    Boolean btchBookg = grpHdr.isBtchBookg();
    if (btchBookg == null) {
      stmt.setString(3, null);
    } else {
      stmt.setString(3, btchBookg.toString());
    }

    // Number of transactions
    stmt.setString(4, grpHdr.getNbOfTxs());

    // Control sum
    BigDecimal ctrlSum = grpHdr.getCtrlSum();
    if (ctrlSum == null) {
      stmt.setString(5, null);
    } else {
      stmt.setString(5, ctrlSum.toString());
    }

    // Total Interbank Settlement Amount
    ActiveCurrencyAndAmount ttlIntrBkSttlmAmt = grpHdr.getTtlIntrBkSttlmAmt();
    if (ttlIntrBkSttlmAmt == null) {
      stmt.setNull(6, Types.INTEGER);
    } else {
      int i = parseActiveCurrencyAndAmount(ttlIntrBkSttlmAmt);
      stmt.setInt(6, i);
    }

    // Interbank settlement date
    LocalDate intrBkSttlmDt = grpHdr.getIntrBkSttlmDt();
    if (intrBkSttlmDt == null) {
      stmt.setString(7, null);
    } else {
      stmt.setString(7, intrBkSttlmDt.toString());
    }

    // Settlement Information
    SettlementInstruction4 sttlmInf = grpHdr.getSttlmInf();
    int settlementInformation = parseSettlementInformation(sttlmInf);
    stmt.setInt(8, settlementInformation);

    // Payment Type Information
    PaymentTypeInformation21 pmtTpInf = grpHdr.getPmtTpInf();
    if (pmtTpInf == null) {
      stmt.setNull(9, Types.INTEGER);
    } else {
      int i = parsePaymentTypeInformation(pmtTpInf);
      stmt.setInt(9, i);
    }

    // Instructing agent
    BranchAndFinancialInstitutionIdentification5 instgAgt = grpHdr.getInstgAgt();
    if (instgAgt == null) {
      stmt.setNull(10, Types.INTEGER);
    } else {
      int i = parseBranchAndFinancialInstitutionIdentification(instgAgt);
      stmt.setInt(10, i);
    }

    // Instructed agent
    BranchAndFinancialInstitutionIdentification5 instdAgt = grpHdr.getInstdAgt();
    if (instdAgt == null) {
      stmt.setNull(11, Types.INTEGER);
    } else {
      int i = parseBranchAndFinancialInstitutionIdentification(instdAgt);
      stmt.setInt(11, i);
    }

    stmt.executeUpdate();

    return getId(stmt);
  }

}
