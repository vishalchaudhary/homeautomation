from flask import Flask
from lifx import lifx
app = Flask(__name__)

app.register_blueprint(lifx, url_prefix='/lifx')

if __name__ == '__main__':
    app.run(debug=False)
