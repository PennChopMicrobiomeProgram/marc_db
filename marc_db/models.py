from sqlalchemy import Column, Integer, Text, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Isolate(Base):
    __tablename__ = "isolates"

    sample_id = Column(Text, primary_key=True)
    subject_id = Column(Integer, nullable=False)
    specimen_id = Column(Integer, nullable=False)
    source = Column(Text)
    suspected_organism = Column(Text, default="unknown")
    special_collection = Column(Text)
    received_date = Column(Date, nullable=True)
    cryobanking_date = Column(Date, nullable=True)


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
    isolate_id = Column(Text, ForeignKey("isolates.sample_id"), nullable=False)
    tube_barcode = Column(Text)
    box_name = Column(Text)
