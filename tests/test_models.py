import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from marc_db.models import Base, Isolate, Aliquot


@pytest.fixture(scope="module")
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture(scope="module")
def session(engine):
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_isolate_model(session):
    new_isolate = Isolate(
        subject_id=1,
        specimen_id=1,
        source="Blood",
        suspected_organism="unknown",
        special_collection="bacteremia",
        received_date="2021-01-01",
        cryobanking_date="2021-01-02",
    )
    session.add(new_isolate)
    session.commit()

    isolate = session.query(Isolate).first()
    assert isolate.subject_id == 1
    assert isolate.specimen_id == 1
    assert isolate.source == "Blood"
    assert isolate.suspected_organism == "unknown"
    assert isolate.special_collection == "bacteremia"
    assert isolate.received_date == "2021-01-01"
    assert isolate.cryobanking_date == "2021-01-02"


def test_aliquot_model(session):
    new_isolate = Isolate(
        subject_id=1,
        specimen_id=1,
        source="Blood",
        suspected_organism="unknown",
        special_collection="bacteremia",
        received_date="2021-01-01",
        cryobanking_date="2021-01-02",
    )
    session.add(new_isolate)
    session.commit()

    new_aliquot = Aliquot(
        isolate_id=new_isolate.id, tube_barcode="123456", box_name="Box1"
    )
    session.add(new_aliquot)
    session.commit()

    aliquot = session.query(Aliquot).first()
    assert aliquot.isolate_id == new_isolate.id
    assert aliquot.tube_barcode == "123456"
    assert aliquot.box_name == "Box1"
