from flask import Flask, jsonify
from flask_cors import CORS


app = Flask(__name__)
CORS(app=app)

@app.route('/api/home', methods=['GET'])
def api_home():
    return jsonify({
        'message': 'Hello, Q.'
    })


if __name__ == '__main__':
    app.run(debug=True, port=8080)
