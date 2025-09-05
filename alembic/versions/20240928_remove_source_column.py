"""remove source column from isolates"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20240928_remove_source"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("isolates", "source")


def downgrade() -> None:
    op.add_column("isolates", sa.Column("source", sa.Text(), nullable=True))
