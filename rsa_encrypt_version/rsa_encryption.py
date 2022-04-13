# -*- coding: utf-8 -*-

from Crypto.Cipher import PKCS1_OAEP, AES
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes


def create_rsa_keys(code):
    """
    生成RSA私钥和公钥
    :param code: 密码
    :return:
    """
    # 生成 2048 位的 RSA 密钥
    key = RSA.generate(2048)
    encrypted_key = key.exportKey(passphrase=code, pkcs=8, protection="scryptAndAES128-CBC")
    # 生成私钥
    with open('import_es_data/private_rsa_key.bin', 'wb') as f:
        f.write(encrypted_key)
    # 生成公钥
    with open('export_es_data/rsa_public.pem', 'wb') as f:
        f.write(key.publickey().exportKey())


def file_encryption(file_name, public_key):
    """
    文件加密
    :param file_name: 文件路径名
    :param public_key: 公钥
    :return:
    """
    # 二进制只读打开文件，读取文件数据
    with open(file_name, 'rb') as f:
        data = f.read()
    file_name_new = file_name + '.rsa'
    with open(file_name_new, 'wb') as out_file:
        # 收件人秘钥 - 公钥
        recipient_key = RSA.import_key(open(public_key).read())
        # 一个 16 字节的会话密钥
        session_key = get_random_bytes(16)
        # Encrypt the session key with the public RSA key
        cipher_rsa = PKCS1_OAEP.new(recipient_key)
        out_file.write(cipher_rsa.encrypt(session_key))
        # Encrypt the data with the AES session key
        cipher_aes = AES.new(session_key, AES.MODE_EAX)
        cipher_text, tag = cipher_aes.encrypt_and_digest(data)
        out_file.write(cipher_aes.nonce)
        out_file.write(tag)
        out_file.write(cipher_text)
    return file_name_new


def file_decryption(file_name, code, private_key):
    """
    文件解密
    :param file_name: 文件路径名
    :param code: 密码
    :param private_key: 私钥
    :return:
    """
    with open(file_name, 'rb') as f_in:
        # 导入私钥
        private_key = RSA.import_key(open(private_key).read(), passphrase=code)
        # 会话密钥, 随机数, 消息认证码, 机密的数据
        enc_session_key, nonce, tag, cipher_text = [f_in.read(x) for x in (private_key.size_in_bytes(), 16, 16, -1)]
        cipher_rsa = PKCS1_OAEP.new(private_key)
        session_key = cipher_rsa.decrypt(enc_session_key)
        cipher_aes = AES.new(session_key, AES.MODE_EAX, nonce)
        # 解密
        data = cipher_aes.decrypt_and_verify(cipher_text, tag)
    # 文件重命名
    out_file_name = file_name.replace('.rsa', '')
    with open(out_file_name, 'wb') as f_out:
        f_out.write(data)
    return out_file_name


if __name__ == '__main__':
    create_rsa_keys("lmmK6dt399>IlmZpv3mmO188hTE$2002")
    # file_encryption("test.txt", "rsa_public.pem")
    # file_decryption("test.txt.rsa", "lmmK6dt399>IlmZpv3mmO188hTE$2002", "private_rsa_key.bin")