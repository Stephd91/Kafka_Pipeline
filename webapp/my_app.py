from flask import Flask, render_template
import pandas as pd
import requests

webapp = Flask(__name__)

url = "http://127.0.0.1:1111/batch/footdata"
response = requests.get(url)
json_content = response.json()
print(json_content[0])
data = []

# Extract data and create a DataFrame
for entry in json_content[0]:
    competition = entry.get("competition").get("name")
    hometeam = entry.get("homeTeam").get("name")
    awayteam = entry.get("awayTeam").get("name")
    match_date = entry.get("utcDate")

    data.append([competition, hometeam, awayteam, match_date])

df = pd.DataFrame(data, columns=["competition", "hometeam", "awayteam", "match_date"])
df["match_date"] = (
    pd.to_datetime(df["match_date"])
    .dt.tz_convert("Europe/Paris")
    .dt.strftime("%d-%m-%Y %H:%M")
)

print(df.head(3))


@webapp.route("/")
def index():
    # Render the DataFrame as an HTML table in the web page
    table_html = df.to_html(index=False)
    return render_template("index.html", table=table_html)


if __name__ == "__main__":
    webapp.run(debug=True, port=2222)
