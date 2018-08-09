from flask import Flask

from main import home_bp

def create_app():

    app = Flask(__name__)
    app.debug = True

    app.register_blueprint(home_bp)

    return app

