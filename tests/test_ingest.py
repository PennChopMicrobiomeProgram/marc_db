import pandas as pd
import pytest
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from marc_db.ingest import ingest_from_tsvs
from marc_db.models import Base, Aliquot, Isolate
from marc_db.views import get_isolates, get_aliquots


data_dir = Path(__file__).parent


@pytest.fixture(scope="module")
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture(scope="module")
def session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    yield session
    session.close()


@pytest.fixture(scope="module")
def ingest(session):
    isolates_df = pd.read_csv(data_dir / "test_multi_aliquot.tsv", sep="\t")
    ingest_from_tsvs(isolates=isolates_df, yes=True, session=session)
    return session


def test_ingest_isolates(ingest):
    assert len(get_isolates(ingest)) == 2


def test_views(ingest):
    assert len(get_isolates(ingest, sample_id="sample1")) == 1
    assert len(get_aliquots(ingest, id=1)) == 1


def test_conflicting_duplicate_rows():
    engine = create_engine("sqlite:///:memory:")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    isolates_df = pd.read_csv(data_dir / "test_bad_duplicates.tsv", sep="\t")
    ingest_from_tsvs(isolates=isolates_df, yes=True, session=session)

    isolate = session.query(Isolate).filter_by(sample_id="sample1").one()
    assert isolate.subject_id == 1
    assert session.query(Aliquot).count() == 2

    session.close()
    engine.dispose()


def test_ingest_accepts_path_strings():
    engine = create_engine("sqlite:///:memory:")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    tsv_path = str(data_dir / "test_multi_aliquot.tsv")
    ingest_from_tsvs(isolates=tsv_path, yes=True, session=session)

    assert len(get_isolates(session)) == 2
    assert len(get_aliquots(session)) == 5

    session.close()
    engine.dispose()


def test_duplicate_isolate_rows_do_not_warn_when_identical(capsys):
    engine = create_engine("sqlite:///:memory:")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    isolates_df = pd.read_csv(data_dir / "test_multi_aliquot.tsv", sep="\t")

    ingest_from_tsvs(isolates=isolates_df, yes=True, session=session)

    captured = capsys.readouterr()
    assert "Conflicting isolate data" not in captured.out
    assert len(get_isolates(session)) == 2
    assert len(get_aliquots(session)) == 5

    session.close()
    engine.dispose()


def test_ingest_bacteremia_example(tmp_path):
    engine = create_engine("sqlite:///:memory:")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    bacteremia_tsv = """SampleID\tsample species\tReceived by mARC\tCryobanking\tsample_source\tNote\tTechnician\tspecial_collection\tTube Type\tTube Barcode\tBox-name_position\tSubject ID\tSpecimen ID
marc.bacteremia.1\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164439\tmARC Bacteremia Isolates Box 1\t1.0\t1.0
marc.bacteremia.2\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164431\tmARC Bacteremia Isolates Box 1\t2.0\t2.0
marc.bacteremia.3\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164460\tmARC Bacteremia Isolates Box 1\t1.0\t1.0
marc.bacteremia.4\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164429\tmARC Bacteremia Isolates Box 1\t3.0\t3.0
marc.bacteremia.5\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164455\tmARC Bacteremia Isolates Box 1\t4.0\t4.0
marc.bacteremia.6\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164452\tmARC Bacteremia Isolates Box 1\t2.0\t5.0
marc.bacteremia.7\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164446\tmARC Bacteremia Isolates Box 1\t2.0\t5.0
marc.bacteremia.8\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164454\tmARC Bacteremia Isolates Box 1\t5.0\t6.0
marc.bacteremia.9\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164472\tmARC Bacteremia Isolates Box 1\t6.0\t7.0
marc.bacteremia.10\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164444\tmARC Bacteremia Isolates Box 1\t6.0\t8.0
marc.bacteremia.11\tKlebsiella pneumoniae\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164428\tmARC Bacteremia Isolates Box 1\t7.0\t9.0
marc.bacteremia.12\tEscherichia coli\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164468\tmARC Bacteremia Isolates Box 1\t8.0\t10.0
marc.bacteremia.13\tEnterobacter cloacae\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164462\tmARC Bacteremia Isolates Box 1\t9.0\t11.0
marc.bacteremia.14\tUnknown\t2022-03-09 00:00:00\t2022-03-09 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164450\tmARC Bacteremia Isolates Box 1\t10.0\t12.0
marc.bacteremia.15\tUnknown\t2022-03-09 00:00:00\t2022-03-09 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113164467\tmARC Bacteremia Isolates Box 1\t11.0\t13.0
marc.bacteremia.16\tPseudomonas aeruginosa\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165151\tmARC Bacteremia Isolates Box 1\t12.0\t14.0
marc.bacteremia.17\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165140\tmARC Bacteremia Isolates Box 1\t13.0\t15.0
marc.bacteremia.18\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165159\tmARC Bacteremia Isolates Box 1\t14.0\t16.0
marc.bacteremia.19\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165166\tmARC Bacteremia Isolates Box 1\t14.0\t17.0
marc.bacteremia.20\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165167\tmARC Bacteremia Isolates Box 1\t14.0\t18.0
marc.bacteremia.21\tStaphylococcus aureus\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165144\tmARC Bacteremia Isolates Box 1\t15.0\t19.0
marc.bacteremia.22\tEscherichia coli\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165158\tmARC Bacteremia Isolates Box 1\t16.0\t20.0
marc.bacteremia.23\tEscherichia coli\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165182\tmARC Bacteremia Isolates Box 1\t16.0\t20.0
marc.bacteremia.24\tStaphylococcus aureus\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165177\tmARC Bacteremia Isolates Box 1\t15.0\t21.0
marc.bacteremia.25\tStaphylococcus aureus\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165181\tmARC Bacteremia Isolates Box 1\t15.0\t22.0
marc.bacteremia.26\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165162\tmARC Bacteremia Isolates Box 1\t14.0\t23.0
marc.bacteremia.27\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165180\tmARC Bacteremia Isolates Box 1\t14.0\t24.0
marc.bacteremia.28\tStaphylococcus aureus\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165161\tmARC Bacteremia Isolates Box 1\t15.0\t25.0
marc.bacteremia.29\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165153\tmARC Bacteremia Isolates Box 1\t14.0\t26.0
marc.bacteremia.30\tUnknown\t2022-03-16 00:00:00\t2022-03-16 00:00:00\tblood culture\t\tT'Nia\tBacteremia\ta\tNA2113165169\tmARC Bacteremia Isolates Box 1\t17.0\t27.0
marc.bacteremia.1\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164442\tmARC Bacteremia Isolates Box 1\t1.0\t1.0
marc.bacteremia.2\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164449\tmARC Bacteremia Isolates Box 1\t2.0\t2.0
marc.bacteremia.3\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164427\tmARC Bacteremia Isolates Box 1\t1.0\t1.0
marc.bacteremia.4\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113163791\tmARC Bacteremia Isolates Box 1\t3.0\t3.0
marc.bacteremia.5\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164465\tmARC Bacteremia Isolates Box 1\t4.0\t4.0
marc.bacteremia.6\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164435\tmARC Bacteremia Isolates Box 1\t2.0\t5.0
marc.bacteremia.7\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164456\tmARC Bacteremia Isolates Box 1\t2.0\t5.0
marc.bacteremia.8\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164445\tmARC Bacteremia Isolates Box 1\t5.0\t6.0
marc.bacteremia.9\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164447\tmARC Bacteremia Isolates Box 1\t6.0\t7.0
marc.bacteremia.10\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164457\tmARC Bacteremia Isolates Box 1\t6.0\t8.0
marc.bacteremia.11\tKlebsiella pneumoniae\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164463\tmARC Bacteremia Isolates Box 1\t7.0\t9.0
marc.bacteremia.12\tEscherichia coli\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164434\tmARC Bacteremia Isolates Box 1\t8.0\t10.0
marc.bacteremia.13\tEnterobacter cloacae\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164470\tmARC Bacteremia Isolates Box 1\t9.0\t11.0
marc.bacteremia.14\tUnknown\t2022-03-09 00:00:00\t2022-03-09 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164432\tmARC Bacteremia Isolates Box 1\t10.0\t12.0
marc.bacteremia.15\tUnknown\t2022-03-09 00:00:00\t2022-03-09 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113164453\tmARC Bacteremia Isolates Box 1\t11.0\t13.0
marc.bacteremia.16\tPseudomonas aeruginosa\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165142\tmARC Bacteremia Isolates Box 1\t12.0\t14.0
marc.bacteremia.17\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165147\tmARC Bacteremia Isolates Box 1\t13.0\t15.0
marc.bacteremia.18\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165143\tmARC Bacteremia Isolates Box 1\t14.0\t16.0
marc.bacteremia.19\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165174\tmARC Bacteremia Isolates Box 1\t14.0\t17.0
marc.bacteremia.20\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165173\tmARC Bacteremia Isolates Box 1\t14.0\t18.0
marc.bacteremia.21\tStaphylococcus aureus\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165183\tmARC Bacteremia Isolates Box 1\t15.0\t19.0
marc.bacteremia.22\tEscherichia coli\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165179\tmARC Bacteremia Isolates Box 1\t16.0\t20.0
marc.bacteremia.23\tEscherichia coli\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165165\tmARC Bacteremia Isolates Box 1\t16.0\t20.0
marc.bacteremia.24\tStaphylococcus aureus\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165178\tmARC Bacteremia Isolates Box 1\t15.0\t21.0
marc.bacteremia.25\tStaphylococcus aureus\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165172\tmARC Bacteremia Isolates Box 1\t15.0\t22.0
marc.bacteremia.26\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165171\tmARC Bacteremia Isolates Box 1\t14.0\t23.0
marc.bacteremia.27\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165163\tmARC Bacteremia Isolates Box 1\t14.0\t24.0
marc.bacteremia.28\tStaphylococcus aureus\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165152\tmARC Bacteremia Isolates Box 1\t15.0\t25.0
marc.bacteremia.29\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165164\tmARC Bacteremia Isolates Box 1\t14.0\t26.0
marc.bacteremia.30\tUnknown\t2022-03-16 00:00:00\t2022-03-16 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tb\tNA2113165157\tmARC Bacteremia Isolates Box 1\t17.0\t27.0
marc.bacteremia.1\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113159912\tmARC Bacteremia Isolates Box 1\t1.0\t1.0
marc.bacteremia.2\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164461\tmARC Bacteremia Isolates Box 1\t2.0\t2.0
marc.bacteremia.3\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164458\tmARC Bacteremia Isolates Box 1\t1.0\t1.0
marc.bacteremia.4\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164471\tmARC Bacteremia Isolates Box 1\t3.0\t3.0
marc.bacteremia.5\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164466\tmARC Bacteremia Isolates Box 1\t4.0\t4.0
marc.bacteremia.6\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113161266\tmARC Bacteremia Isolates Box 1\t2.0\t5.0
marc.bacteremia.7\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164451\tmARC Bacteremia Isolates Box 1\t2.0\t5.0
marc.bacteremia.8\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164464\tmARC Bacteremia Isolates Box 1\t5.0\t6.0
marc.bacteremia.9\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164436\tmARC Bacteremia Isolates Box 1\t6.0\t7.0
marc.bacteremia.10\tUnknown\t2022-03-04 00:00:00\t2022-03-04 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113163803\tmARC Bacteremia Isolates Box 1\t6.0\t8.0
marc.bacteremia.11\tKlebsiella pneumoniae\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164440\tmARC Bacteremia Isolates Box 1\t7.0\t9.0
marc.bacteremia.12\tEscherichia coli\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164473\tmARC Bacteremia Isolates Box 1\t8.0\t10.0
marc.bacteremia.13\tEnterobacter cloacae\t2022-03-07 00:00:00\t2022-03-07 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164430\tmARC Bacteremia Isolates Box 1\t9.0\t11.0
marc.bacteremia.14\tUnknown\t2022-03-09 00:00:00\t2022-03-09 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164433\tmARC Bacteremia Isolates Box 1\t10.0\t12.0
marc.bacteremia.15\tUnknown\t2022-03-09 00:00:00\t2022-03-09 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113164437\tmARC Bacteremia Isolates Box 1\t11.0\t13.0
marc.bacteremia.16\tPseudomonas aeruginosa\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165146\tmARC Bacteremia Isolates Box 1\t12.0\t14.0
marc.bacteremia.17\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165139\tmARC Bacteremia Isolates Box 1\t13.0\t15.0
marc.bacteremia.18\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165148\tmARC Bacteremia Isolates Box 1\t14.0\t16.0
marc.bacteremia.19\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165149\tmARC Bacteremia Isolates Box 1\t14.0\t17.0
marc.bacteremia.20\tStaphylococcus epidermidis\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165170\tmARC Bacteremia Isolates Box 1\t14.0\t18.0
marc.bacteremia.21\tStaphylococcus aureus\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165176\tmARC Bacteremia Isolates Box 1\t15.0\t19.0
marc.bacteremia.22\tEscherichia coli\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165138\tmARC Bacteremia Isolates Box 1\t16.0\t20.0
marc.bacteremia.23\tEscherichia coli\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165185\tmARC Bacteremia Isolates Box 1\t16.0\t20.0
marc.bacteremia.24\tStaphylococcus aureus\t2022-03-11 00:00:00\t2022-03-11 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165156\tmARC Bacteremia Isolates Box 1\t15.0\t21.0
marc.bacteremia.25\tStaphylococcus aureus\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165154\tmARC Bacteremia Isolates Box 1\t15.0\t22.0
marc.bacteremia.26\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165184\tmARC Bacteremia Isolates Box 1\t14.0\t23.0
marc.bacteremia.27\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165160\tmARC Bacteremia Isolates Box 1\t14.0\t24.0
marc.bacteremia.28\tStaphylococcus aureus\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165175\tmARC Bacteremia Isolates Box 1\t15.0\t25.0
marc.bacteremia.29\tUnknown\t2022-03-14 00:00:00\t2022-03-14 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165150\tmARC Bacteremia Isolates Box 1\t14.0\t26.0
marc.bacteremia.30\tUnknown\t2022-03-16 00:00:00\t2022-03-16 00:00:00\tblood culture\t\tT'Nia\tBacteremia\tc\tNA2113165168\tmARC Bacteremia Isolates Box 1\t17.0\t27.0
"""

    tsv_path = tmp_path / "bacteremia.tsv"
    tsv_path.write_text(bacteremia_tsv)

    ingest_from_tsvs(isolates=str(tsv_path), yes=True, session=session)

    assert len(get_isolates(session)) == 30
    assert len(get_aliquots(session)) == 90

    session.close()
    engine.dispose()
