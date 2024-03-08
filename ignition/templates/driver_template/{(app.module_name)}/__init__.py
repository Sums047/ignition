import json
from os.path import dirname
from .app import create_app

with open(dirname(__file__) + '/pkg_info.json') as fp:
    _pkg_info = json.load(fp)

__version__ = _pkg_info['version']


async def create_wsgi_app():
    ignition_app = await create_app()
    # For wsgi deployments
    return ignition_app.connexion_app
