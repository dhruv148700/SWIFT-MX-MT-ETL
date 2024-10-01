package smx.parser;

import com.prowidesoftware.swift.model.mx.dic.*;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

import static smx.parser.Pacs00800107Parser.connection;

public class MessageElementParsers {

  public static int getId(PreparedStatement stmt) throws SQLException {
    ResultSet result = stmt.getGeneratedKeys();
    int generatedId = -1;
    if (result.next()) {
      generatedId = result.getInt(1);
    }
    return generatedId;
  }

  /**
   * Loads payment type information into PaymentTypeInformation Table
   * @param pmtTpInf - PaymentTypeInformation21 element (Can be null)
   */
  public static int parsePaymentTypeInformation(PaymentTypeInformation21 pmtTpInf) throws SQLException {
    String query = "INSERT INTO PaymentTypeInformation(InstrPrty, ClrChanl, SvcLvlCd, " +
            "SvcLvlPrtry, LclInstrmCd, LclInstrmPrtry, CtgyPurpCd, CtgyPurpPrtry) " +
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, pmtTpInf.getInstrPrty().toString());
    stmt.setString(2, pmtTpInf.getClrChanl().toString());

    ServiceLevel8Choice svcLvl = pmtTpInf.getSvcLvl();
    if (svcLvl == null) {
      stmt.setString(3, null);
      stmt.setString(4, null);
    } else {
      stmt.setString(3, svcLvl.getCd());
      stmt.setString(4, svcLvl.getPrtry());
    }

    LocalInstrument2Choice lclInstrm = pmtTpInf.getLclInstrm();
    if (lclInstrm == null) {
      stmt.setString(5, null);
      stmt.setString(6, null);
    } else {
      stmt.setString(5, lclInstrm.getCd());
      stmt.setString(6, lclInstrm.getPrtry());
    }

    CategoryPurpose1Choice ctgyPurp = pmtTpInf.getCtgyPurp();
    if (ctgyPurp == null) {
      stmt.setString(7, null);
      stmt.setString(8, null);
    } else {
      stmt.setString(7, ctgyPurp.getCd());
      stmt.setString(8, ctgyPurp.getPrtry());
    }

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Loads generic financial data into GenericFinancialIdentification Table
   * @param genFinId - GenericFinancialIdentification1 element (Can be null)
   */
  private static int parseGenericFinancialIdentificaiton(GenericFinancialIdentification1 genFinId) throws SQLException {
    String query = "INSERT INTO GenericFinancialIdentification" +
            "(Identification, Cd, Prtry, Issr) VALUES(?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, genFinId.getId());

    FinancialIdentificationSchemeName1Choice schmeNm = genFinId.getSchmeNm();
    if (schmeNm == null) {
      stmt.setString(2, null);
      stmt.setString(3, null);
    } else {
      stmt.setString(2, schmeNm.getCd());
      stmt.setString(3, schmeNm.getPrtry());
    }

    stmt.setString(4, genFinId.getIssr());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Loads postal address data into PostalAddress Table
   * @param pstlAdr - PostalAddress6 element (Can be null)
   */
  public static int parsePostalAddress(PostalAddress6 pstlAdr) throws SQLException {
    String query = "INSERT INTO PostalAddress(AdrTp, Dept, SubDept, StrtNm, " +
            "BldgNb, PstCd, TwnNm, CtrySubDvsn, Ctry, AdrLine) VALUES(?, ?, " +
            "?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, pstlAdr.getAdrTp().toString());
    stmt.setString(2, pstlAdr.getDept());
    stmt.setString(3, pstlAdr.getSubDept());
    stmt.setString(4, pstlAdr.getStrtNm());
    stmt.setString(5, pstlAdr.getBldgNb());
    stmt.setString(6, pstlAdr.getPstCd());
    stmt.setString(7, pstlAdr.getTwnNm());
    stmt.setString(8, pstlAdr.getCtrySubDvsn());
    stmt.setString(9, pstlAdr.getCtry());

    StringBuilder sb = new StringBuilder();
    for (String s : pstlAdr.getAdrLine()) {
      sb.append(s);
    }
    stmt.setString(10, sb.toString());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load clearing system data into ClearingSystemIdentification Table
   * @param clrSys - ClearingSystemIdentification3Choice element (Can be null)
   */
  public static int parseClearingSystem2(ClearingSystemIdentification2Choice clrSys) throws SQLException {
    String query = "INSERT INTO ClearingSystemIdentification(Cd, Prtry) VALUES(?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, clrSys.getCd());
    stmt.setString(2, clrSys.getPrtry());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load clearing system data into ClearingSystemIdentification Table
   * @param clrSys - ClearingSystemIdentification3Choice element (Can be null)
   */
  public static int parseClearingSystem3(ClearingSystemIdentification3Choice clrSys) throws SQLException {
    String query = "INSERT INTO ClearingSystemIdentification(Cd, Prtry) VALUES(?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, clrSys.getCd());
    stmt.setString(2, clrSys.getPrtry());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load cash account data into CashAccount Table
   * @param cashAcct - CashAccount24 element (Can be null)
   */
  public static int parseCashAccount(CashAccount24 cashAcct) throws SQLException {
    String query = "INSERT INTO CashAccount(Identification, Tp, Ccy, Nm) " +
            "VALUES(?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, cashAcct.getId().toString());
    stmt.setString(2, cashAcct.getTp().toString());
    stmt.setString(3, cashAcct.getCcy());
    stmt.setString(4, cashAcct.getNm());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load active currency and amount data into ActiveCurrencyAndAmount Table
   * @param activeCurrencyAndAmount - ActiveCurrencyAndAmount element (Can be
   *                               null)
   */
  public static int parseActiveCurrencyAndAmount(ActiveCurrencyAndAmount activeCurrencyAndAmount) throws SQLException {
    String query = "INSERT INTO ActiveCurrencyAndAmount (ActiveCurrency, CurrencyAmount) VALUES (?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, activeCurrencyAndAmount.getCcy());
    stmt.setString(2, activeCurrencyAndAmount.getValue().toString());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load clearing system member identification into
   * ClearingSystemMemberIdentification Table
   * @param clrSysMmbId - ClearingSystemMemberIdentification2 element (Can be
   *                    null)
   */
  public static int parseClearingSystemMemberIdentification(ClearingSystemMemberIdentification2 clrSysMmbId) throws SQLException {
    String query = "INSERT INTO ClearingSystemMemberIdentification (ClrSysId, MmbId) VALUES (?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    ClearingSystemIdentification2Choice clrSysId = clrSysMmbId.getClrSysId();
    if (clrSysId == null) {
      stmt.setNull(1, Types.INTEGER);
    } else {
      int clearingSystemId = parseClearingSystem2(clrSysId);
      stmt.setInt(1, clearingSystemId);
    }

    stmt.setString(2, clrSysMmbId.getMmbId());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load branch data into BranchIdentification Table
   * @param brnchId - BranchData2 element (Can be null)
   */
  private static int parseBranchIdentification(BranchData2 brnchId) throws SQLException {
    String query = "INSERT INTO BranchIdentification(Identification, Nm, " +
            "PstlAdr) VALUES(?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, brnchId.getId());
    stmt.setString(2, brnchId.getNm());

    PostalAddress6 pstlAdr = brnchId.getPstlAdr();
    if (pstlAdr == null) {
      stmt.setNull(3, Types.INTEGER);
    } else {
      int postalAddressId = parsePostalAddress(pstlAdr);
      stmt.setInt(3, postalAddressId);
    }

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load financial institution data into FinancialInstitutionIdentification
   * Table
   * @param finInstnId - FinancialInstitutionIdentification8 element (Can be
   *                   null)
   */
  private static int parseFinancialInstitutionIdentification(FinancialInstitutionIdentification8 finInstnId) throws SQLException {
    String query = "INSERT INTO FinancialInstitutionIdentification(BICFI, " +
            "ClrSysMmbId, Nm, PstlAdr, Othr) VALUES(?, ?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, finInstnId.getBICFI());

    ClearingSystemMemberIdentification2 clrSysMmbId = finInstnId.getClrSysMmbId();
    if (clrSysMmbId == null) {
      stmt.setNull(2, Types.INTEGER);
    } else {
      int clearingSystemMemberIdentificationId =
              parseClearingSystemMemberIdentification(clrSysMmbId);
      stmt.setInt(2, clearingSystemMemberIdentificationId);
    }

    stmt.setString(3, finInstnId.getNm());

    PostalAddress6 pstlAdr = finInstnId.getPstlAdr();
    if (pstlAdr == null) {
      stmt.setNull(4, Types.INTEGER);
    } else {
      int postalAddressId = parsePostalAddress(pstlAdr);
      stmt.setInt(4, postalAddressId);
    }

    GenericFinancialIdentification1 othr = finInstnId.getOthr();
    if (othr == null) {
      stmt.setNull(5, Types.INTEGER);
    } else {
      int genericFinancialIdentificaitonId =
              parseGenericFinancialIdentificaiton(othr);
      stmt.setNull(5, genericFinancialIdentificaitonId);
    }

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load branch and financial institution data into
   * BranchAndFinancialInstitutionIdentification Table
   * @param id - BranchAndFinancialInstitutionIdentification5 element
   *           (Can be null)
   */
  public static int parseBranchAndFinancialInstitutionIdentification(BranchAndFinancialInstitutionIdentification5 id) throws SQLException {
    String query = "INSERT INTO BranchAndFinancialInstitutionIdentification " +
            "(FinInstnId, BrnchId) VALUES(?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    FinancialInstitutionIdentification8 finInstnId = id.getFinInstnId();
    if (finInstnId == null) {
      stmt.setNull(1, Types.INTEGER);
    } else {
      int financialInstitutionId =
              parseFinancialInstitutionIdentification(finInstnId);
      stmt.setInt(1, financialInstitutionId);
    }

    BranchData2 brnchId = id.getBrnchId();
    if (brnchId == null) {
      stmt.setNull(2, Types.INTEGER);
    } else {
      int branchIdentification = parseBranchIdentification(brnchId);
      stmt.setInt(2, branchIdentification);
    }

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load Settlement Information into SettlementInstruction Table
   * @param sttlmInf - SettlementInstruction4 element (Non null)
   */
  public static int parseSettlementInformation(SettlementInstruction4 sttlmInf) throws SQLException {

    String query = "INSERT INTO SettlementInstruction(SttlmMtd, SttlmAcct, " +
            "ClrSys, InstgRmbrsmntAgt, InstgRmbrsmntAgtAcct, " +
            "InstdRmbrsmntAgt, InstdRmbrsmntAgtAcct, ThrdRmbrsmntAgt, " +
            "ThrdRmbrsmntAgtAcct) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    // Settlement method code
    stmt.setString(1, sttlmInf.getSttlmMtd().toString());

    // Settlement account
    CashAccount24 sttlmAcct = sttlmInf.getSttlmAcct();
    if (sttlmAcct == null) {
      stmt.setNull(2, Types.INTEGER);
    } else{
      int cashAccountId = parseCashAccount(sttlmAcct);
      stmt.setInt(2, cashAccountId);
    }

    // Clearing System
    ClearingSystemIdentification3Choice clrSys = sttlmInf.getClrSys();
    if (clrSys == null) {
      stmt.setNull(3, Types.INTEGER);
    } else {
      int clearingSystem3Id = parseClearingSystem3(clrSys);
      stmt.setInt(3, clearingSystem3Id);
    }

    // Instructing Reimbursement Agent
    BranchAndFinancialInstitutionIdentification5 instgRmbrsmntAgt = sttlmInf.getInstgRmbrsmntAgt();
    if (instgRmbrsmntAgt == null) {
      stmt.setNull(4, Types.INTEGER);
    } else {
      int i = parseBranchAndFinancialInstitutionIdentification(instgRmbrsmntAgt);
      stmt.setInt(4, i);
    }

    // Instructing Reimbursement Agent Account
    CashAccount24 instgRmbrsmntAgtAcct = sttlmInf.getInstgRmbrsmntAgtAcct();
    if (instgRmbrsmntAgtAcct == null) {
      stmt.setNull(5, Types.INTEGER);
    } else {
      int i = parseCashAccount(instgRmbrsmntAgtAcct);
      stmt.setInt(5, i);
    }

    // Instructed Reimbursement Agent
    BranchAndFinancialInstitutionIdentification5 instdRmbrsmntAgt = sttlmInf.getInstdRmbrsmntAgt();
    if (instdRmbrsmntAgt == null) {
      stmt.setNull(6, Types.INTEGER);
    } else {
      int i = parseBranchAndFinancialInstitutionIdentification(instdRmbrsmntAgt);
      stmt.setInt(6, i);
    }

    // Instructed Reimbursement Agent Account
    CashAccount24 instdRmbrsmntAgtAcct = sttlmInf.getInstdRmbrsmntAgtAcct();
    if (instdRmbrsmntAgtAcct == null) {
      stmt.setNull(7, Types.INTEGER);
    } else {
      int i = parseCashAccount(instdRmbrsmntAgtAcct);
      stmt.setInt(7, i);
    }

    // Third Reimbursement Agent
    BranchAndFinancialInstitutionIdentification5 thrdRmbrsmntAgt = sttlmInf.getThrdRmbrsmntAgt();
    if (thrdRmbrsmntAgt == null) {
      stmt.setNull(8, Types.INTEGER);
    } else {
      int i = parseBranchAndFinancialInstitutionIdentification(thrdRmbrsmntAgt);
      stmt.setInt(8, i);
    }

    // Third Reimbursement Agent Account
    CashAccount24 thrdRmbrsmntAgtAcct = sttlmInf.getThrdRmbrsmntAgtAcct();
    if (thrdRmbrsmntAgtAcct == null) {
      stmt.setNull(9, Types.INTEGER);
    } else {
      int i = parseCashAccount(thrdRmbrsmntAgtAcct);
      stmt.setInt(9, i);
    }

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load Payment Identification data into the PaymentIdentification Table
   * @param pmtId - PaymentIdentification3 element (Non null)
   */
  public static int parsePaymentIdentification(PaymentIdentification3 pmtId) throws SQLException {
    String query = "INSERT INTO PaymentIdentification(InstrId, EndToEndId, " +
            "TxId, ClrSysRef) VALUES(?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query,
            PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, pmtId.getInstrId());
    stmt.setString(2, pmtId.getEndToEndId());
    stmt.setString(3, pmtId.getTxId());
    stmt.setString(4, pmtId.getClrSysRef());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load Settlement Date and Time into the SettlementTimeIndication Table
   * @param sttlmTmIndctn - SettlementDateTimeIndication1 element (Can be null)
   */
  public static int parseSettlementDateTime(SettlementDateTimeIndication1 sttlmTmIndctn) throws SQLException {
    String query = "INSERT INTO SettlementTimeIndication(DbtDtTm, CdtDtTm) " +
            "VALUES(?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query,
            PreparedStatement.RETURN_GENERATED_KEYS);

    // Debit date time
    OffsetDateTime dbtDtTm = sttlmTmIndctn.getDbtDtTm();
    if (dbtDtTm == null) {
      stmt.setString(1, null);
    } else {
      stmt.setString(1, dbtDtTm.toString());
    }

    // Credit date time
    OffsetDateTime cdtDtTm = sttlmTmIndctn.getCdtDtTm();
    if (cdtDtTm == null) {
      stmt.setString(2, null);
    } else {
      stmt.setString(2, cdtDtTm.toString());
    }

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load Settlement Time Request data into the SettlementTimeRequest Table
   * @param sttlmTmReq - SettlementTimeRequest2 element (Can be null)
   */
  public static int parseSettlementTimeRequest(SettlementTimeRequest2 sttlmTmReq) throws SQLException {
    String query = "INSERT INTO SettlementTimeRequest(CLSTm, TillTm, FrTm, " +
            "RjctTm) VALUES(?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    // CLS Time
    OffsetTime clsTm = sttlmTmReq.getCLSTm();
    if (clsTm == null) {
      stmt.setString(1, null);
    } else {
      stmt.setString(1, clsTm.toString());
    }

    // Till Time
    OffsetTime tillTm = sttlmTmReq.getTillTm();
    if (clsTm == null) {
      stmt.setString(2, null);
    } else {
      stmt.setString(2, tillTm.toString());
    }

    // From Time
    OffsetTime frTm = sttlmTmReq.getFrTm();
    if (clsTm == null) {
      stmt.setString(3, null);
    } else {
      stmt.setString(3, frTm.toString());
    }

    // Reject Time
    OffsetTime rjctTm = sttlmTmReq.getRjctTm();
    if (clsTm == null) {
      stmt.setString(4, null);
    } else {
      stmt.setString(4, rjctTm.toString());
    }

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load Active Or Historic Currency and Amount into ActiveCurrencyAndAmount
   * @param camt - ActiveOrHistoricCurrencyAndAmount element (Can be null)
   */
  public static int parseActiveOrHistoricCurrencyAmount(ActiveOrHistoricCurrencyAndAmount camt) throws SQLException {
    String query = "INSERT INTO ActiveCurrencyAndAmount(ActiveCurrency, " +
            "CurrencyAmount) VALUES(?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    stmt.setString(1, camt.getCcy());
    stmt.setString(2, camt.getValue().toString());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load Contact Details into the ContactDetails Table
   * @param ctctDtls - ContactDetails2 element (Can be null)
   */
  public static int parseContactDetails(ContactDetails2 ctctDtls) throws SQLException {
    String query = "INSERT INTO ContactDetails(NmPrfx, Nm, PhneNb, MobNb, " +
            "FaxNb, EmailAdr, Othr) VALUES(?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement stmt = connection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);

    // Name prefix
    NamePrefix1Code nmPrfx = ctctDtls.getNmPrfx();
    if (nmPrfx == null) {
      stmt.setString(1, null);
    } else {
      stmt.setString(1, nmPrfx.toString());
    }

    // Name
    stmt.setString(2, ctctDtls.getNm());

    // Phone Number
    stmt.setString(3, ctctDtls.getPhneNb());

    // Mobile Number
    stmt.setString(4, ctctDtls.getMobNb());

    // Fax Number
    stmt.setString(5, ctctDtls.getFaxNb());

    // Email Address
    stmt.setString(6, ctctDtls.getEmailAdr());

    // Other
    stmt.setString(7, ctctDtls.getOthr());

    stmt.executeUpdate();

    return getId(stmt);
  }

  /**
   * Load Organisation Identification data into OrganisationIdentification
   * @param orgId - OrganisationIdentification8 element (Non null)
   */
  private static void parseOrganisationIdentification(OrganisationIdentification8 orgId) {
    // BIC
    String anyBIC = orgId.getAnyBIC();
    System.out.println("BIC: " + anyBIC);

    for (GenericOrganisationIdentification1 o : orgId.getOthr()) {
      // Identification
      String id = o.getId();
      System.out.println("Identification: " + id);

      // Scheme Name
      OrganisationIdentificationSchemeName1Choice schmeNm = o.getSchmeNm();
      System.out.println("Scheme Name: ");

      // Code
      String cd = schmeNm.getCd();
      System.out.println("Code: " + cd);

      // Proprietary
      String prtry = schmeNm.getPrtry();
      System.out.println("Proprietary: " + prtry);

      // Issuer
      String issr = o.getIssr();
      System.out.println("Issuer: " + issr);
    }
  }

  private static void parseBirthDateAndPlace(DateAndPlaceOfBirth1 dtAndPlcOfBirth) {
    if (dtAndPlcOfBirth == null) {
      System.out.println("null");
    } else {
      // Birth Date
      LocalDate birthDt = dtAndPlcOfBirth.getBirthDt();
      System.out.println("Birth Date: " + birthDt);

      // Province of Birth
      String prvcOfBirth = dtAndPlcOfBirth.getPrvcOfBirth();
      System.out.println("Birth Province: " + prvcOfBirth);

      // City of Birth
      String cityOfBirth = dtAndPlcOfBirth.getCityOfBirth();
      System.out.println("Birth City: " + cityOfBirth);

      // Country of Birth
      String ctryOfBirth = dtAndPlcOfBirth.getCtryOfBirth();
      System.out.println("Birth Country: " + ctryOfBirth);
    }
  }

  private static void parsePersonIdentification(PersonIdentification13 prvtId) {
    // Date and Place of Birth
    DateAndPlaceOfBirth1 dtAndPlcOfBirth = prvtId.getDtAndPlcOfBirth();
    System.out.print("Date and Place of Birth: ");
    parseBirthDateAndPlace(dtAndPlcOfBirth);

    for (GenericPersonIdentification1 p : prvtId.getOthr()) {
      // Identification
      String id = p.getId();
      System.out.println("Identification: " + id);

      // Scheme name
      PersonIdentificationSchemeName1Choice schmeNm = p.getSchmeNm();
      System.out.println("Scheme Name: ");

      // Code
      String cd = schmeNm.getCd();
      System.out.println("Code: " + cd);

      // Proprietary
      String prtry = schmeNm.getPrtry();
      System.out.println("Proprietary: " + prtry);

      // Issuer
      String issr = p.getIssr();
      System.out.println("Issuer: " + issr);
    }
  }


  private static void parseIdentification(Party34Choice id) {
    if (id == null) {
      System.out.println("null");
    } else {
      // Organisation Identification
      OrganisationIdentification8 orgId = id.getOrgId();
      System.out.println("\nOrganisation Identification: ");
      parseOrganisationIdentification(orgId);

      // Private Identification
      PersonIdentification13 prvtId = id.getPrvtId();
      System.out.println("Private Identification: ");
      parsePersonIdentification(prvtId);
    }
  }

  public static void parsePartyIdentification(PartyIdentification125 partyId) {
    if (partyId == null) {
      System.out.println("null");
    } else {
      // Name
      String nm = partyId.getNm();
      System.out.println("\nName: " + nm);

      // Postal Address
      PostalAddress6 pstlAdr = partyId.getPstlAdr();
      // parsePostalAddress(pstlAdr);

      // Identification
      Party34Choice id = partyId.getId();
      System.out.print("Identification: ");
      parseIdentification(id);

      // Country of Residence
      String ctryOfRes = partyId.getCtryOfRes();
      System.out.println("Country of Residence: " + ctryOfRes);

      // Contact Details
      ContactDetails2 ctctDtls = partyId.getCtctDtls();
      System.out.print("Contact details: ");
      parseContactDetails(ctctDtls);
    }
  }

}
