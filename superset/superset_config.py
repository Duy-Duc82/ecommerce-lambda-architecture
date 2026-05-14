import os

# --- Core Settings ---
SECRET_KEY = 'e-commerce-big-data-lambda-super-secret-key-12345'
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# --- Customization ---
APP_NAME = "E-Commerce Business Intelligence"
APP_ICON_TRANSPARENT = False
LOGO_TARGET_PATH = '/'

# --- Feature Flags ---
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# --- Cache ---
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': 'redis://redis:6379/1'
}
DATA_CACHE_CONFIG = CACHE_CONFIG

# --- Timezone ---
BABEL_DEFAULT_LOCALE = 'en'
BABEL_DEFAULT_FOLDER = 'superset/translations'
LANGUAGES = {
    'en': {'flag': 'us', 'name': 'English'},
}
