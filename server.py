import os
from trialstreamer import config

FLASK_ENV = os.environ.get('FLASK_ENV', '')

if FLASK_ENV != 'development':
    from gevent import monkey
    monkey.patch_all()

from trialstreamer.cnxapp import app

flask_app = app.app
