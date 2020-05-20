/*
 * Copyright (c) 2019, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

public class PgSQLXMLTest extends BaseTest4 {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    TestUtil.createTempTable(con, "xmltab","x xml");
  }

  @Test
  public void setCharacterStream() throws  Exception {
    String exmplar = "<x>value</x>";
    SQLXML pgSQLXML = con.createSQLXML();
    Writer writer = pgSQLXML.setCharacterStream();
    writer.write(exmplar);
    PreparedStatement preparedStatement = con.prepareStatement("insert into xmltab values (?)");
    preparedStatement.setSQLXML(1,pgSQLXML);
    preparedStatement.execute();

    Statement statement = con.createStatement();
    ResultSet rs = statement.executeQuery("select * from xmltab");
    assertTrue(rs.next());
    SQLXML result = rs.getSQLXML(1);
    assertNotNull(result);
    assertEquals(exmplar, result.getString());
  }

  /*
  see  https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html
   */
  @Test
  public void testXXEGetSource() throws Exception {
    try {
      PgSQLXML x = new PgSQLXML(null, "<!DOCTYPE foo [<!ELEMENT foo ANY >\n"
          + "<!ENTITY xxe SYSTEM \"file:///etc/hosts\">]><foo>&xxe;</foo>");
      x.getSource(null);
    } catch ( SQLException ex ) {
      assertTrue("Expected to get a DOCTYPE disallowed SAXParseException", ex.getCause().getMessage().startsWith("DOCTYPE is disallowed"));
    }
  }

  @Test
  public void setSaxResult() throws Exception {
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer identityTransformer = tf.newTransformer();
    try {
      PgSQLXML xml = new PgSQLXML(null);
      Result result = xml.setResult(SAXResult.class);

      Source source = new StreamSource(new StringReader("<!DOCTYPE foo [<!ELEMENT foo ANY >\n"
          + "<!ENTITY xxe SYSTEM \"file:///etc/hosts\">]><foo>&xxe;</foo>"));
    } catch ( Exception ex ) {
      assertTrue("Expected to get a DOCTYPE disallowed SAXParseException", ex.getCause().getMessage().startsWith("DOCTYPE is disallowed"));
    }
  }
}
