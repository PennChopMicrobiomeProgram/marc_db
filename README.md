# marc_db

A simple database and query interface for the microbial archive

## Usage

### CLI

```
pip install .
export MARC_DB_URL=sqlite:////path/to/db.sqlite  # optional environment variable
marc_db -h
marc_db --db sqlite:////path/to/db.sqlite ingest /path/to/data_anonymized.tsv
```

This will create a new database at `/path/to/db.sqlite` and ingest the anonymized data from marc_honest at `/path/to/data_anonymized.tsv`.

### Library

Import SQLAlchemy data models with `from marc_db.models import Aliquot, Isolate`. Either create your own database connection (e.g. with flask_sqlalchemy if using Flask) or import with `from marc_db.db import get_session`. Query the database or import and use the provided views:

```
from marc_db.db import get_session
from marc_db.models import Aliquot, Isolate
from marc_db.views import get_isolates

session = get_session()
print(session.query(Isolate).limit(10))
print(get_isolates(session, n = 10))
```