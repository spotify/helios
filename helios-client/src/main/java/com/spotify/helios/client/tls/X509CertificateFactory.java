/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.client.tls;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;

import com.eaio.uuid.UUID;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.oiw.OIWObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509ExtensionUtils;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.DigestCalculator;
import org.bouncycastle.operator.bc.BcDigestCalculatorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.common.Hash.sha1;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class X509CertificateFactory {

  private static final Path HELIOS_HOME = Paths.get(System.getProperty("user.home"), ".helios");

  private static final BaseEncoding HEX_ENCODING = BaseEncoding.base16().lowerCase();

  private static final Logger log = LoggerFactory.getLogger(X509CertificateFactory.class);

  private static final JcaX509CertificateConverter CERTIFICATE_CONVERTER =
      new JcaX509CertificateConverter().setProvider("BC");

  private static final BaseEncoding KEY_ID_ENCODING =
      BaseEncoding.base16().upperCase().withSeparator(":", 2);

  private static final int KEY_SIZE = 2048;

  private final Path cacheDirectory;
  private final int validBeforeMilliseconds;
  private final int validAfterMilliseconds;

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  public X509CertificateFactory() {
    this(HELIOS_HOME, (int) TimeUnit.HOURS.toMillis(1), (int) TimeUnit.HOURS.toMillis(48));
  }

  public X509CertificateFactory(final Path cacheDirectory, final int validBeforeMilliseconds,
                                final int validAfterMillieconds) {
    this.cacheDirectory = cacheDirectory;
    this.validBeforeMilliseconds = validBeforeMilliseconds;
    this.validAfterMilliseconds = validAfterMillieconds;
  }

  public CertificateAndPrivateKey get(final AgentProxy agentProxy, final Identity identity,
                                      final String username) {
    final MessageDigest identityHash = sha1();
    identityHash.update(identity.getKeyBlob());
    identityHash.update(username.getBytes());

    final String identityHex = HEX_ENCODING.encode(identityHash.digest()).substring(0, 8);
    final Path cacheCertPath = cacheDirectory.resolve(identityHex + ".crt");
    final Path cacheKeyPath = cacheDirectory.resolve(identityHex + ".pem");

    boolean useCached = false;
    CertificateAndPrivateKey cached = null;

    try {
      if (Files.exists(cacheCertPath) && Files.exists(cacheKeyPath)) {
        cached = CertificateAndPrivateKey.from(cacheCertPath, cacheKeyPath);
      }
    } catch (IOException | GeneralSecurityException e) {
      // some sort of issue with cached certificate, that's fine
      log.debug("error reading cached certificate and key from {} for identity={}",
                cacheDirectory, identity.getComment(), e);
    }

    if ((cached != null) && (cached.getCertificate() instanceof X509Certificate)) {
      final X509Certificate cachedX509 = (X509Certificate) cached.getCertificate();
      final Date now = new Date();

      if (now.after(cachedX509.getNotBefore()) && now.before(cachedX509.getNotAfter())) {
        useCached = true;
      }
    }

    if (useCached) {
      log.debug("using existing certificate for {} from {}", username, cacheCertPath);
      return cached;
    } else {
      final CertificateAndPrivateKey generated = generate(agentProxy, identity, username);
      saveToCache(cacheDirectory, cacheCertPath, cacheKeyPath, generated);

      return generated;
    }
  }

  private CertificateAndPrivateKey generate(final AgentProxy agentProxy, final Identity identity,
                                            final String username) {

    final UUID uuid = new UUID();
    final Calendar calendar = Calendar.getInstance();
    final X500Name issuerDN = new X500Name("C=US,O=Spotify,CN=helios-client");
    final X500Name subjectDN = new X500NameBuilder().addRDN(BCStyle.UID, username).build();

    calendar.add(Calendar.MILLISECOND, -validBeforeMilliseconds);
    final Date notBefore = calendar.getTime();

    calendar.add(Calendar.MILLISECOND, validBeforeMilliseconds + validAfterMilliseconds);
    final Date notAfter = calendar.getTime();

    // Reuse the UUID time as a SN
    final BigInteger serialNumber = BigInteger.valueOf(uuid.getTime()).abs();

    try {
      final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
      keyPairGenerator.initialize(KEY_SIZE, new SecureRandom());

      final KeyPair keyPair = keyPairGenerator.generateKeyPair();
      final SubjectPublicKeyInfo subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(
          ASN1Sequence.getInstance(keyPair.getPublic().getEncoded()));

      final X509v3CertificateBuilder builder = new X509v3CertificateBuilder(issuerDN, serialNumber,
                                                                            notBefore, notAfter,
                                                                            subjectDN,
                                                                            subjectPublicKeyInfo);

      final DigestCalculator digestCalculator = new BcDigestCalculatorProvider()
          .get(new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1));
      final X509ExtensionUtils utils = new X509ExtensionUtils(digestCalculator);

      final SubjectKeyIdentifier keyId = utils.createSubjectKeyIdentifier(subjectPublicKeyInfo);
      final String keyIdHex = KEY_ID_ENCODING.encode(keyId.getKeyIdentifier());
      log.info("generating an X509 certificate for {} with key ID={} and identity={}",
               username, keyIdHex, identity.getComment());

      builder.addExtension(Extension.subjectKeyIdentifier, false, keyId);
      builder.addExtension(Extension.authorityKeyIdentifier, false,
                           utils.createAuthorityKeyIdentifier(subjectPublicKeyInfo));
      builder.addExtension(Extension.keyUsage, false,
                           new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyCertSign));
      builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));

      final X509CertificateHolder holder = builder.build(new SshAgentContentSigner(agentProxy,
                                                                                   identity));

      final X509Certificate certificate = CERTIFICATE_CONVERTER.getCertificate(holder);
      log.debug("generated certificate:\n{}", asPEMString(certificate));

      return new CertificateAndPrivateKey(certificate, keyPair.getPrivate());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static void saveToCache(final Path cacheDirectory,
                                  final Path cacheCertPath,
                                  final Path cacheKeyPath,
                                  final CertificateAndPrivateKey certificateAndPrivateKey) {
    try {
      Files.createDirectories(cacheDirectory);

      final String certPem = asPEMString(certificateAndPrivateKey.getCertificate());
      final String keyPem = asPEMString(certificateAndPrivateKey.getPrivateKey());

      // overwrite any existing file, and make sure it's only readable by the current user
      final Set<StandardOpenOption> options = ImmutableSet.of(CREATE, WRITE);
      final Set<PosixFilePermission> perms = ImmutableSet.of(PosixFilePermission.OWNER_READ,
                                                             PosixFilePermission.OWNER_WRITE);
      final FileAttribute<Set<PosixFilePermission>> attrs =
          PosixFilePermissions.asFileAttribute(perms);

      try (final SeekableByteChannel sbc =
               Files.newByteChannel(cacheCertPath, options, attrs)) {
        sbc.write(ByteBuffer.wrap(certPem.getBytes()));
      }

      try (final SeekableByteChannel sbc =
               Files.newByteChannel(cacheKeyPath, options, attrs)) {
        sbc.write(ByteBuffer.wrap(keyPem.getBytes()));
      }

      log.debug("cached generated certificate to {}", cacheCertPath);
    } catch (IOException e) {
      // couldn't save to the cache, oh well
      log.warn("error caching generated certificate", e);
    }
  }

  private static String asPEMString(final Object o) throws IOException {
    final StringWriter sw = new StringWriter();

    try (final JcaPEMWriter pw = new JcaPEMWriter(sw)) {
      pw.writeObject(o);
    }

    return sw.toString();
  }
}
