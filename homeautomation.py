from flask import Flask, render_template, session, request
# from flask_socketio import SocketIO, emit, join_room, leave_room, \
#     close_room, rooms, disconnect
from lifx import lifx

# async_mode = None
app = Flask(__name__)
# app.config['SECRET_KEY'] = 'secret!'
# socketio = SocketIO(app, async_mode=async_mode)
# thread = None
app.register_blueprint(lifx, url_prefix='/lifx')


# def background_thread():
#     """Example of how to send server generated events to clients."""
#     count = 0
#     while True:
#         socketio.sleep(10)
#         count += 1
#         socketio.emit('my_response',
#                       {'data': 'Server generated event', 'count': count},
#                       namespace='/test')
#
#
# def send_message(message):
#     socketio.emit('my_response',
#                   {'data': message, 'count': 1},
#                   namespace='/test')
#     print message+"asdasdasd"


# def send_message(message):
#     """Example of how to send server generated events to clients."""
#     print message
#     count = 0
#     count += 1
#
#     socketio.emit('my_response',
#                   {'data': message, 'count': count},
#                   namespace='/test')
@app.route('/')
def index():
    return render_template('index.html')


# @socketio.on('my_event', namespace='/test')
# def test_message(message):
#     session['receive_count'] = session.get('receive_count', 0) + 1
#     emit('my_response',
#          {'data': message['data'], 'count': session['receive_count']})
#
#
# @socketio.on('my_broadcast_event', namespace='/test')
# def test_broadcast_message(message):
#     print message
#     session['receive_count'] = session.get('receive_count', 0) + 1
#     emit('my_response',
#          {'data': message['data'], 'count': session['receive_count']},
#          broadcast=True)
#
#
# @socketio.on('join', namespace='/test')
# def join(message):
#     join_room(message['room'])
#     session['receive_count'] = session.get('receive_count', 0) + 1
#     emit('my_response',
#          {'data': 'In rooms: ' + ', '.join(rooms()),
#           'count': session['receive_count']})
#
#
# @socketio.on('leave', namespace='/test')
# def leave(message):
#     leave_room(message['room'])
#     session['receive_count'] = session.get('receive_count', 0) + 1
#     emit('my_response',
#          {'data': 'In rooms: ' + ', '.join(rooms()),
#           'count': session['receive_count']})
#
#
# @socketio.on('close_room', namespace='/test')
# def close(message):
#     session['receive_count'] = session.get('receive_count', 0) + 1
#     emit('my_response', {'data': 'Room ' + message['room'] + ' is closing.',
#                          'count': session['receive_count']},
#          room=message['room'])
#     close_room(message['room'])
#
#
# @socketio.on('my_room_event', namespace='/test')
# def send_room_message(message):
#     session['receive_count'] = session.get('receive_count', 0) + 1
#     emit('my_response',
#          {'data': message['data'], 'count': session['receive_count']},
#          room=message['room'])
#
#
# @socketio.on('disconnect_request', namespace='/test')
# def disconnect_request():
#     session['receive_count'] = session.get('receive_count', 0) + 1
#     emit('my_response',
#          {'data': 'Disconnected!', 'count': session['receive_count']})
#     disconnect()
#
#
# @socketio.on('my_ping', namespace='/test')
# def ping_pong():
#     emit('my_pong')
#
#
# @socketio.on('connect', namespace='/test')
# def test_connect():
#     global thread
#     if thread is None:
#         thread = socketio.start_background_task(target=background_thread)
#     emit('my_response', {'data': 'Connected', 'count': 0})
#
#
# @socketio.on('disconnect', namespace='/test')
# def test_disconnect():
#     print('Client disconnected', request.sid)


if __name__ == '__main__':
     app.run(host='0.0.0.0',debug=False)
    #socketio.run(app)
