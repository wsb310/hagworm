# -*- coding: utf-8 -*-

import binascii
import base64
import jwt
import textwrap

from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend


class RsaUtil:
    """Rsa加解密相关工具类
    """

    @classmethod
    def gen_rsa_key(cls, rsa_key, private=False):

        if private:
            start_line = r'-----BEGIN RSA PRIVATE KEY-----'
            end_line = r'-----END RSA PRIVATE KEY-----'
        else:
            start_line = r'-----BEGIN PUBLIC KEY-----'
            end_line = r'-----END PUBLIC KEY-----'

        rsa_key = textwrap.fill(rsa_key, 64)

        return '\n'.join([start_line, rsa_key, end_line])

    @classmethod
    def rsa_sign(cls, rsa_key, sign_data):

        algorithm = jwt.algorithms.RSAAlgorithm(hashes.SHA1)

        key = algorithm.prepare_key(cls.gen_rsa_key(rsa_key, True))

        signature = algorithm.sign(sign_data.encode(r'utf-8'), key)

        return base64.b64encode(signature).decode()

    @classmethod
    def rsa_verity(cls, pubic_key, verity_data, verity_sign):

        algorithm = jwt.algorithms.RSAAlgorithm(hashes.SHA1)

        public_key = load_pem_public_key(cls.gen_rsa_key(pubic_key).encode(r'utf-8'), backend=default_backend())

        result = algorithm.verify(verity_data.encode(), public_key, binascii.a2b_base64(verity_sign))

        return result
