"""App key and secret."""

import os

# --- credentials: injected at build time via scripts/update_credentials.py — do not edit ---
def _r(d: tuple[int, ...]) -> str:
    _k = (109, 97, 109, 109, 111, 116, 105, 111, 110, 95, 97, 112, 112)
    return bytes(v ^ _k[i % len(_k)] for i, v in enumerate(d)).decode()

APP_KEY = _r((94, 85, 95, 92, 86, 68, 91, 88))
APP_SECRET = _r((8, 2, 14, 88, 92, 71, 15, 87, 94, 104, 4, 69, 67, 91, 88, 15, 92, 13, 77, 13, 11, 91, 110, 87, 73, 20, 91, 7, 14, 88, 92, 64))
MAMMOTION_OAUTH2_CLIENT_ID = _r((42, 25, 8, 15, 8, 39, 29, 87, 29, 54, 87, 0, 59, 28, 51))
MAMMOTION_OAUTH2_CLIENT_SECRET = _r((39, 49, 93, 88, 95, 76, 58, 61, 36, 25, 0, 64, 49, 84, 81, 44, 41, 31, 14, 37, 38, 32, 27, 35, 8, 61, 12, 85, 59, 7))
# --- end credentials ---

if not APP_KEY:
    from dotenv import load_dotenv

    load_dotenv()
    APP_KEY = os.environ.get("ALIYUN_APP_KEY", "34231230")
    APP_SECRET = os.environ.get("ALIYUN_APP_SECRET", "1ba85698bb10e19c6437413b61ba3445")

if not MAMMOTION_OAUTH2_CLIENT_ID:
    MAMMOTION_OAUTH2_CLIENT_ID = os.environ.get("MAMMOTION_OAUTH2_CLIENT_ID", "")
    MAMMOTION_OAUTH2_CLIENT_SECRET = os.environ.get("MAMMOTION_OAUTH2_CLIENT_SECRET", "")

APP_VERSION = os.environ.get("APP_VERSION", "2.3.8.19")
ALIYUN_DOMAIN = "api.link.aliyun.com"
MAMMOTION_DOMAIN = "https://id.mammotion.com"
MAMMOTION_API_DOMAIN = "https://domestic.mammotion.com"
MAMMOTION_CLIENT_ID = "MADKALUBAS"
MAMMOTION_CLIENT_SECRET = "GshzGRZJjuMUgd2sYHM7"
