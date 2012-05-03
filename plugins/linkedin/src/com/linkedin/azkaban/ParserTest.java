package com.linkedin.azkaban;

import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.xml.sax.InputSource;

public class ParserTest {
    public ParserTest() {
        
    }
    
    public static void main(String[] args) {
        String values = "<?xml version='1.0' encoding='UTF-8'?><mp xmlns=\"http://www.linkedin.com/profile/v3\" mID=\"23770640\" dloc=\"en_US\" ct=\"1208127500000\" lmt=\"1323110681180\" naID=\"21\"><pss /><lcn><g>0</g><pc>WK02</pc><cc>bm</cc></lcn><locs><c eID=\"7\" ct=\"1208152700000\" vis=\"E\" lmt=\"1222763694347\" /><les sz=\"1\"><le loc=\"en_US\" locv=\"1\" lmt=\"1222763694347\" langf=\"U\" ct=\"1208152700000\" /></les></locs><fn><c eID=\"2\" ct=\"1208152700000\" vis=\"E\" lmt=\"1323110681180\" /><les sz=\"1\"><le loc=\"en_US\" locv=\"1\" lmt=\"1323110681180\"><![CDATA[Deirdre]]></le></les></fn><ln><c eID=\"3\" ct=\"1208152700000\" vis=\"E\" lmt=\"1323110681180\" /><les sz=\"1\"><le loc=\"en_US\" locv=\"1\" lmt=\"1323110681180\"><![CDATA[Mellamphy]]></le></les></ln><mn /><hl><c eID=\"4\" ct=\"1208152700000\" vis=\"E\" lmt=\"1323110681180\" /><les sz=\"1\"><le loc=\"en_US\" locv=\"1\" lmt=\"1323110681180\" uc=\"1\"><![CDATA[VP Finance - TeleBermuda International Limited]]></le></les></hl><ind><c eID=\"1\" ct=\"1208152700000\" vis=\"E\" lmt=\"1323110681180\" ind=\"8\" /></ind><sum><c eID=\"9\" ct=\"1262181810333\" vis=\"E\" lmt=\"1282013032438\" /><les sz=\"1\"><le loc=\"en_US\" locv=\"1\" lmt=\"1282013032438\"><![CDATA[Deirdre has over 14 years of accounting experience obtained while working in various industries including insurance &amp; pensions, e-commerce and hospitality.  She gained her initial telecommunications experience as Group Finance Manager at Zintel Communications Limited, an innovative provider of voice, business phone systems &amp; network infrastructure solutions in New Zealand.  Deirdre has held various accounting positions, including Financial Accountant for Sovereign Assurance Ltd, Financial Controller at Global-e Investments Ltd, and Financial Accountant for Equitable Life Insurance Company Limited.  Prior to her current position at TBI, she had her own business contracting as a project accountant, with roles including conversion of financial statements and reporting structures from national reporting standards to International Financial Reporting Standards (IFRS), forming business &amp; strategic plans and system implementation.]]></le></les></sum><asn><c eID=\"12\" ct=\"1262181810333\" vis=\"E\" lmt=\"1282013623624\" /><les sz=\"1\"><le loc=\"en_US\" locv=\"1\" lmt=\"1282013623624\"><![CDATA[New Zealand Institute of Chartered Accountants, Bermuda Police Womens Rugby Club.]]></le></les></asn><int><c eID=\"11\" ct=\"1262181810333\" vis=\"E\" lmt=\"1282013623624\" /><les sz=\"1\"><le loc=\"en_US\" locv=\"1\" lmt=\"1282013623624\"><![CDATA[Travelling, rugby, long distance running, swimming, wine, reading.]]>";
        
        if (values instanceof String) {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader((String)values));
            try {
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                dBuilder.parse(is);
            } catch (Exception e) {
                System.err.println(e.toString());
            }
            
        }
    }
}