# DigitalOcean Spaces FastAPI Browser

A minimal FastAPI application that:

1. Displays a table of objects in a DigitalOcean Spaces bucket via plain HTML
2. Generates signed (presigned) URLs for secure object downloads
3. Runs a background monitor that records an **immutable changelog** of additions, modifications, and deletions in the bucket.

---

## Prerequisites

* Python 3.9+
* A DigitalOcean Space and access credentials

## Setup

1. **Clone** this repository.

2. **Create** a `.env` file with your credentials (you can copy `example.env`):

```bash
cp example.env .env
```

Edit `.env` and fill in:

```
DO_ACCESS_KEY_ID=...
DO_SECRET_KEY=...
DO_ENDPOINT=https://nyc3.digitaloceanspaces.com  # or your region
DO_BUCKET=my-space-name
```

3. **Install** dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

4. **Run** the app:

```bash
uvicorn main:app --reload --port 5052
```

The HTML table will be available at `http://127.0.0.1:8000`.

---

## Changelog & Snapshots

* A directory called `data/` will be created automatically.
* `data/snapshot.json` stores the latest bucket state.
* `data/changelog.jsonl` stores an **append-only** log; each line is a JSON object describing a change.

Example entry:

```json
{"action": "added", "key": "images/cat.png", "time": "2024-05-27T18:27:13.214Z"}
```

---

## Customisation

* Adjust the polling interval by changing the `interval` argument in `bucket_monitor` inside `main.py` (defaults to 60 s).
* Modify the HTML/CSS in `templates/index.html` to suit your style.

---

## License

MIT 