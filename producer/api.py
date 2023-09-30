import requests
from flask import Flask, jsonify, request
from flask_limiter import Limiter
import os

# Create a Flask web application
app = Flask(__name__)
limiter = Limiter(app)


# Define a route for the root URL "/"
@app.get("/")
def get_hello():
    return "<div align='center'> \
                <h1> Welcome to the event-source side! </h1>\
            </div>"


# Define a route to produce a string message passed as path parameter
@app.get("/somedata/<string:data>")
def send_message(data: str):
    """
    Returns the string that has been passed in the path parameter
    Args: number = defines how many line of the same string it returns
    """
    number = request.args.get("number", type=int, default=1)
    message = {i: data for i in range(number)}
    return jsonify(message)


# Define a route to get data from an external API (https://www.football-data.org/)
# with a 10 call/min limitation
@limiter.limit("10/minute", key_func=lambda: "/footdata")
@app.get("/footdata")
def get_footdata():
    """
    Get data from an external API (https://www.football-data.org/)
    """
    # get the API tken from an environment variable
    my_token = os.environ.get("FOOT_TOKEN")
    headers = {"X-Auth-Token": my_token}
    api_url = "https://api.football-data.org/v4/matches"
    # Get the list of football matches of the day
    response = requests.get(api_url, headers=headers, timeout=60)
    output_data = response.json()["matches"]
    matches_oftheday = list(output_data)
    # if match["area"]["name"] == "France" # if we want only French games
    return matches_oftheday


if __name__ == "__main__":
    # Run the Flask app on localhost (127.0.0.1) and port 5000
    app.run(host="127.0.0.1", port=5000, debug=True)
