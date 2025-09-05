import argparse
from datetime import datetime
from marc_db.db import get_session
from marc_db.models import Aliquot, Base, Isolate
from sqlalchemy.orm import Session
from typing import Optional


isolate1 = Isolate(
    sample_id="sample1",
    subject_id=1,
    specimen_id=1,
    suspected_organism="K. pneumonia",
    special_collection="none",
    received_date=datetime(2021, 1, 1),
    cryobanking_date=datetime(2021, 1, 2),
)
isolate2 = Isolate(sample_id="sample2", subject_id=1, specimen_id=2)
isolate3 = Isolate(
    sample_id="sample3",
    subject_id=2,
    specimen_id=1,
    suspected_organism="E. coli",
    special_collection="none",
    received_date=datetime(2021, 1, 3),
    cryobanking_date=datetime(2021, 1, 4),
)

aliquot1 = Aliquot(isolate_id="sample1", tube_barcode="123", box_name="box1")
aliquot2 = Aliquot(isolate_id="sample1", tube_barcode="124", box_name="box1")
aliquot3 = Aliquot(isolate_id="sample2", tube_barcode="125", box_name="box1")
aliquot4 = Aliquot(isolate_id="sample2", tube_barcode="126", box_name="box1")
aliquot5 = Aliquot(isolate_id="sample3", tube_barcode="127", box_name="box1")
aliquot6 = Aliquot(isolate_id="sample3", tube_barcode="128", box_name="box1")
aliquot7 = Aliquot(isolate_id="sample3", tube_barcode="129", box_name="box1")
aliquot8 = Aliquot(isolate_id="sample3", tube_barcode="130", box_name="box1")


def fill_mock_db(session: Optional[Session] = None):
    if session is None:
        session = get_session()
    # Check that db is an empty test db
    assert (
        len(session.query(Isolate).all()) == 0
    ), "Database is not empty, I can only add test data to an empty database"

    session.add_all(
        [
            isolate1,
            isolate2,
            isolate3,
            aliquot1,
            aliquot2,
            aliquot3,
            aliquot4,
            aliquot5,
            aliquot6,
            aliquot7,
            aliquot8,
        ]
    )
    session.commit()
