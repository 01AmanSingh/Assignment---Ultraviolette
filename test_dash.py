from dash import Dash, html

app = Dash(__name__)
app.layout = html.Div("Dash is working!")

if __name__ == "__main__":
    print("About to start Dash...")
    app.run_server(host="127.0.0.1", port=8050, debug=False)
