/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.ssl;

import org.postgresql.PGProperty;
import org.postgresql.core.PGStream;
import org.postgresql.ssl.jdbc4.LibPQFactory;
import org.postgresql.util.GT;
import org.postgresql.util.ObjectFactory;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

public class MakeSSL extends ObjectFactory {

  private static final Logger LOGGER = Logger.getLogger(MakeSSL.class.getName());

  public static void convert(PGStream stream, Properties info)
      throws PSQLException, IOException {
    LOGGER.log(Level.FINE, "converting regular socket connection to ssl");

    SSLSocketFactory factory;

    String sslmode = PGProperty.SSL_MODE.get(info);

    // Use the default factory if no specific factory is requested
    // unless sslmode is set
    String classname = PGProperty.SSL_FACTORY.get(info);
    if (classname == null) {
      // If sslmode is set, and not disabled use the libpq compatible factory
      if ( sslmode != null &&  !"disable".equalsIgnoreCase(sslmode) ) {
        factory = new LibPQFactory(info);
      } else {
        factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
      }
    } else {
      try {
        factory = (SSLSocketFactory) instantiate(classname, info, true,
            PGProperty.SSL_FACTORY_ARG.get(info));
      } catch (Exception e) {
        throw new PSQLException(
            GT.tr("The SSLSocketFactory class provided {0} could not be instantiated.", classname),
            PSQLState.CONNECTION_FAILURE, e);
      }
    }

    SSLSocket newConnection;
    try {
      newConnection = (SSLSocket) factory.createSocket(stream.getSocket(),
          stream.getHostSpec().getHost(), stream.getHostSpec().getPort(), true);
      // We must invoke manually, otherwise the exceptions are hidden
      newConnection.startHandshake();
    } catch (IOException ex) {
      if (factory instanceof LibPQFactory) { // throw any KeyManager exception
        ((LibPQFactory) factory).throwKeyManagerException();
      }
      throw new PSQLException(GT.tr("SSL error: {0}", ex.getMessage()),
          PSQLState.CONNECTION_FAILURE, ex);
    }

    /*
     * force host name verification unless the user has explicitly asked us not to.
     *
     */
    if ( sslmode != null && !"disable".equalsIgnoreCase(sslmode) ) {
      // did the user give us a HostnameVerifier ?
      String sslhostnameverifier = PGProperty.SSL_HOSTNAME_VERIFIER.get(info);
      if (sslhostnameverifier != null) {
        HostnameVerifier hvn;
        try {
          hvn = (HostnameVerifier) instantiate(sslhostnameverifier, info, false, null);
        } catch (Exception e) {
          throw new PSQLException(
              GT.tr("The HostnameVerifier class provided {0} could not be instantiated.",
                  sslhostnameverifier),
              PSQLState.CONNECTION_FAILURE, e);
        }
        if (!hvn.verify(stream.getHostSpec().getHost(), newConnection.getSession())) {
          throw new PSQLException(
              GT.tr("The hostname {0} could not be verified by hostnameverifier {1}.",
                  stream.getHostSpec().getHost(), sslhostnameverifier),
              PSQLState.CONNECTION_FAILURE);
        }
      } else {
        /*
        * no custom HostnameVerifier and they have asked for verify-full, do all we can to verify the hostname
        * LibPQFactory implements HostnameVerifier; we can use that to verify. otherwise throw
        * and exception
        */

        if ("verify-full".equals(sslmode) ) {
          if (factory instanceof HostnameVerifier) {
            if (!(((HostnameVerifier) factory).verify(stream.getHostSpec().getHost(),
                newConnection.getSession()))) {
              throw new PSQLException(
                  GT.tr("The hostname {0} could not be verified.", stream.getHostSpec().getHost()),
                  PSQLState.CONNECTION_FAILURE);
            }
          } else {
            throw new PSQLException(
                GT.tr("The hostname {0} could not be verified ... no verifier available ",
                    stream.getHostSpec().getHost()),
                PSQLState.CONNECTION_FAILURE);
          }
        }
      }
    }
    stream.changeSocket(newConnection);
  }

}
