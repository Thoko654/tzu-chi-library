import hashlib

# Utility to hash passwords (run this once to generate hashed passwords)
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Example credentials dictionary
users = {
    "admin": hash_password("admin123"),   # replace with your secure password
    "teacher": hash_password("teach2025")
}
