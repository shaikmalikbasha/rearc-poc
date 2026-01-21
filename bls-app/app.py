import os
from datetime import datetime

from flask import Flask, Response, abort, send_from_directory, render_template_string

app = Flask(__name__)

# Root directory where files live
BASE_DATA_DIR = os.path.abspath("data")


@app.route("/")
def index():
    html = """
    <html>
    <head><title>download.bls.gov</title></head>
    <body>
    <div style="text-align: center; margin-top: 100px;">
    <h1>Welcome to the BLS File Server!</h1>
    <button onclick="location.href='/pub/time.series/pr/'">Go to /pub/time.series/pr/</button>
    </div>
    <ul>
    """
    return render_template_string(html)


@app.route("/pub/time.series/pr/", defaults={"filename": None})
@app.route("/pub/time.series/pr/<path:filename>")
def pr_index(filename):
    pr_dir = os.path.join(BASE_DATA_DIR, "pub", "time.series", "pr")

    if not os.path.exists(pr_dir):
        abort(404)

    # Serve file
    if filename:
        file_path = os.path.join(pr_dir, filename)
        if not os.path.isfile(file_path):
            abort(404)
        return send_from_directory(pr_dir, filename, as_attachment=False)

    # Directory listing
    rows = []
    for fname in sorted(os.listdir(pr_dir)):
        fpath = os.path.join(pr_dir, fname)
        if os.path.isfile(fpath):
            stat = os.stat(fpath)
            mtime = datetime.fromtimestamp(stat.st_mtime)

            month = mtime.month
            day = mtime.day
            year = mtime.year
            hour = mtime.hour % 12 or 12
            minute = mtime.minute
            ampm = "AM" if mtime.hour < 12 else "PM"

            date_str = f"{month}/{day}/{year}  {hour}:{minute:02d} {ampm}"

            size = stat.st_size
            rows.append((date_str, size, fname))

    html = [
        "<html><head>",
        "<title>download.bls.gov - /pub/time.series/pr/</title>",
        "</head><body>",
        "<H1>download.bls.gov - /pub/time.series/pr/</H1>",
        "<hr>",
        "<pre>",
        '<A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>',
    ]

    for date, size, name in rows:
        html.append(
            f'{date:20} {size:10} <A HREF="/pub/time.series/pr/{name}">{name}</A><br>'
        )

    html.extend(["</pre><hr>", "</body></html>"])

    return Response("\n".join(html), mimetype="text/html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
