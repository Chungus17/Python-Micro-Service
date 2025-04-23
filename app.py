from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"message": "Yooooo congrats bro your micro service is actually working 😂"})

if __name__ == '__main__':
    app.run()
