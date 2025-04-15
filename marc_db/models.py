from sqlalchemy import Column, Integer, Text, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Isolate(Base):
    __tablename__ = "isolates"
    __tableargs__ = (
        UniqueConstraint(
            "subject_id", "specimen_id", name="uq_isolates_subject_id_specimen_id"
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    subject_id = Column(Integer, nullable=False)
    specimen_id = Column(Integer, nullable=False)
    source = Column(Text)
    suspected_organism = Column(Text, default="unknown")
    special_collection = Column(Text)
    received_date = Column(Date)
    cryobanking_date = Column(Date)


class Aliquot(Base):
    __tablename__ = "aliquots"
    __tableargs__ = (
        UniqueConstraint(
            "isolate_id",
            "tube_barcode",
            "box_name",
            name="uq_aliquots_isolate_id_tube_barcode_box_name",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    isolate_id = Column(Integer, ForeignKey("isolates.id"), nullable=False)
    tube_barcode = Column(Text, nullable=False)
    box_name = Column(Text, nullable=False)
