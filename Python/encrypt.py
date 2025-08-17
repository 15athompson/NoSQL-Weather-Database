import bcrypt
import base64
from cryptography.fernet import Fernet

# Helpers class for Encryption
# Note: Implemented for the purpose of database testing, for production the key
#       itself would need to be stored in a secure manner 
class EncryptionHelper:
    @staticmethod    
    def hash_password(plain_password: str) -> str:
        # Hash a plaintext password using bcrypt with a generated salt.
        # Returns the hash as a UTF-8 string for storage.
        hashed = bcrypt.hashpw(
            plain_password.encode("utf-8"),
            bcrypt.gensalt()
        )
        return hashed.decode("utf-8")  # return as a string
    
    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        # Verify a plaintext password against the stored hashed password.
        return bcrypt.checkpw(
            plain_password.encode("utf-8"),
            hashed_password.encode("utf-8")
        )
    
    @staticmethod
    def get_fernet_key() -> bytes:
        # WARNING: insecure - for testing purposes only
        # Pad or slice to 32 bytes, then base64 encode
        hard_coded_key = "INSECURE_KEY"
        padded = hard_coded_key.ljust(32)[:32].encode("utf-8")
        return base64.urlsafe_b64encode(padded)

    @staticmethod
    def encrypt(plain_text: str) -> str:
        #Encrypts a string and returns a base64-encoded string.
        fernet = Fernet(EncryptionHelper.get_fernet_key())
        return fernet.encrypt(plain_text.encode('utf-8')).decode('utf-8')

    @staticmethod
    def decrypt(encrypted_text: str) -> str:
        # Decrypts an encrypted base64-encoded string.
        fernet = Fernet(EncryptionHelper.get_fernet_key())
        return fernet.decrypt(encrypted_text.encode('utf-8')).decode('utf-8')