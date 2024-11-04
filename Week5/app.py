from flask import Flask, request, jsonify, render_template
import joblib
import numpy as np

app = Flask(__name__)

model = joblib.load("cruise_ship_model.pkl")

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    features = [float(x) for x in request.form.values()]
    final_features= [np.array(features)]
    prediction = model.predict(final_features)

    output = round(prediction[0],4)
    return render_template("index.html",prediction_text="crew should be{}".format(output))
    
if __name__ == '__main__':
    app.run(host = "0.0.0.0", port = 8080, debug=True)