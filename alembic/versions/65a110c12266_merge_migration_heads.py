"""merge migration heads

Revision ID: 65a110c12266
Revises: 20250904_remove_source, 20251028_add_mash_contamination
Create Date: 2026-10-08 18:47:43.629736

"""
from alembic import op
import sqlalchemy as sa
from typing import Sequence, Union


revision: str = '65a110c12266'
down_revision: Union[str, Sequence[str], None] = ('20250904_remove_source', '20251028_add_mash_contamination')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
