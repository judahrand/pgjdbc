---
layout: default_docs
title: Configuring the Client
header: Chapter 4. Using SSL
resource: media
previoustitle: Chapter 4. Using SSL
previous: ssl.html
nexttitle: Custom SSLSocketFactory
next: ssl-factory.html
---

There are a number of connection parameters for configuring the client for ssl. See [SSL Connection parameters](connect.html#ssl)

The simplest being `ssl=true`, passing this into the driver will cause the driver to validate both 
the ssl certificate and verify the hostname (same as `verify-full`). **Note** this is different than
libpq which defaults to a non-validating ssl connection.

In this mode, when establishing a SSL connection the JDBC driver will validate the server's
identity preventing "man in the middle" attacks. It does this by checking that the server
certificate is signed by a trusted authority, and that the host you are connecting to is the
same as the hostname in the certificate.

If you only require encryption then set `sslmode=require`
In the case where the certificate validation is failing you can do one of two things. Either pass
`sslcert=` and LibPQFactory will ignore the client certificate or use the `NonValidatingFactory`
which will trust all server certificates.

The location of the client certificate, client key and root certificate can be overridden with the
`sslcert`, `sslkey`, and `sslrootcert` settings respectively. These default to /defaultdir/postgresql.crt,
/defaultdir/postgresql.pk8, and /defaultdir/root.crt respectively where defaultdir is
${user.home}/.postgresql/ in *nix systems and %appdata%/postgresql/ on windows

Finer control of the SSL connection can be achieved using the `sslmode` connection parameter.
This parameter is the same as the libpq `sslmode` parameter and the currently ssl implements the
following

|sslmode| Eavesdropping Protection| MITM Protection | |
| :---| :--- | :--- | :--- |
| disable | No | No | I don't care about security and don't want to pay the overhead for encryption|
| allow | Maybe | No | I don't care about security but will pay the overhead for encryption if the server insists on it |
| prefer | Maybe | No | I don't care about encryption but will pay the overhead of encryption if the server supports it |
| require | Yes | No | I want my data to be encrypted, and I accept the overhead. I trust that the network will make sure I always connect to the server I want.|
| verify-ca | Yes | Depends on CA policy | I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server that I trust.|
| verify-full | Yes | Yes | I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server I trust, and that it's the one I specify.|


> ### Note

If you are using Java's default mechanism (not LibPQFactory) to create the SSL connection you will
need to make the server certificate available to Java, the first step is to convert
it to a form Java understands.

`openssl x509 -in server.crt -out server.crt.der -outform der`

From here the easiest thing to do is import this certificate into Java's system
truststore.

`keytool -keystore $JAVA_HOME/lib/security/cacerts -alias postgresql -import -file server.crt.der`

The default password for the cacerts keystore is `changeit`. The alias to postgresql
is not important and you may select any name you desire.

If you do not have access to the system cacerts truststore you can create your
own truststore.

`keytool -keystore mystore -alias postgresql -import -file server.crt.der`

When starting your Java application you must specify this keystore and password
to use.

`java -Djavax.net.ssl.trustStore=mystore -Djavax.net.ssl.trustStorePassword=mypassword com.mycompany.MyApp`

In the event of problems extra debugging information is available by adding
`-Djavax.net.debug=ssl` to your command line.

<a name="nonvalidating"></a>
## Using SSL without Certificate Validation

In some situations it may not be possible to configure your Java environment to
make the server certificate available, for example in an applet.  For a large
scale deployment it would be best to get a certificate signed by recognized
certificate authority, but that is not always an option.  The JDBC driver provides
an option to establish a SSL connection without doing any validation, but please
understand the risk involved before enabling this option.

A non-validating connection is established via a custom `SSLSocketFactory` class that is provided
with the driver. Setting the connection URL parameter `sslfactory=org.postgresql.ssl.NonValidatingFactory`
will turn off all SSL validation.
