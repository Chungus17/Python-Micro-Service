from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 👈 This enables CORS for all domains (or specify origins)

@app.route('/')
def home():
    return jsonify({"message": "Yooooo congrats bro your micro service is actually working 😂"})

if __name__ == '__main__':
    app.run()
