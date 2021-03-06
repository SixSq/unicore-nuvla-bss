#
# SSL-related functions
#


import ssl
import re


def setup_ssl(config, socket, LOG, server_mode=False):
    """ Wraps the given socket with an SSL context """
    keystore = config.get('tsi.keystore')
    keypass = config.get('tsi.keypass')
    cert = config.get('tsi.certificate')
    truststore = config.get('tsi.truststore')
    try:
        # TODO: probably it is better not to specify the protocol and instead
        # rely on system defaults!
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = False
        context.load_cert_chain(certfile=cert, keyfile=keystore,
                                password=keypass)
        context.load_verify_locations(cafile=truststore)
        wrapped = context.wrap_socket(socket, server_side=server_mode)
    except AttributeError:
        # Python pre 2.7.9 does not have the SSLContext
        wrapped = ssl.wrap_socket(socket,
                                  cert_reqs=ssl.CERT_REQUIRED,
                                  keyfile=keystore,
                                  certfile=cert,
                                  server_side=server_mode,
                                  ssl_version=ssl.PROTOCOL_TLSv1,
                                  ca_certs=truststore,
                                  ciphers=None)
    return wrapped


rdn_map = {"C": "countryName",
           "CN": "commonName",
           "O": "organizationName",
           "OU": "organizationalUnitName",
           "L": "localityName",
           "ST": "stateOrProvinceName",
           "DC": "domainComponent",
           }


def convert_rdn(rdn):
    split = rdn.split("=")
    translated = rdn_map.get(split[0])
    if translated is None:
        return None
    val = split[1]
    return translated, val


def convert_dn(dn):
    """ Convert X500 DN in RFC format to a tuple """
    converted = []
    # split dn and strip leading/trailing whitespace
    elements = [x.strip() for x in re.split(r"[,/]", dn)]
    for element in elements:
        if element != '':
            rdn = convert_rdn(element)
            if rdn is None:
                # there is no LOG here...
                # LOG.info("*** will not check : %s " % element)
                pass
            converted.append(rdn)
    return converted


def match_rdn(rdn, subject):
    for x in subject:
        for y in x:
            if str(y[0]) == str(rdn[0]) and str(y[1]) == str(rdn[1]):
                return True
    return False


def match(subject, acl):
    for dn in acl:
        accept = True
        # every RDN of the ACL entry has to be in the subject
        for rdn in dn:
            accept = accept and match_rdn(rdn, subject)
            if not accept:
                break
        if accept:
            return True
    return False


def verify_peer(config, socket, LOG):
    """ check that the peer is OK by comparing the DN to our ACL """
    acl = config.get('tsi.allowed_dns', [])
    subject = socket.getpeercert()['subject']
    LOG.debug("Verify XNJS certificate with subject %s" % str(subject))
    if not match(subject, acl):
        raise EnvironmentError("Connection not allowed by ACL")
