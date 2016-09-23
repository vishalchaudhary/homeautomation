from flask import jsonify

__author__ = 'vishi'

from . import lifx
from funcs import *
@lifx.route('/discover/')
def discover():
    discovery()
    return jsonify({'status': 'success'})
