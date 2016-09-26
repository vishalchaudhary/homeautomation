from flask import Flask
from lifx import lifx

app = Flask(__name__)
app.register_blueprint(lifx, url_prefix='/lifx')

if __name__ == '__main__':
     app.run(host='0.0.0.0',debug=False)
    #socketio.run(app)
