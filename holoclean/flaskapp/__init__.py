from flask import Flask

from main import bp

def create_app():

    app = Flask(__name__)
    app.debug = True

    app.register_blueprint(bp)

    return app

