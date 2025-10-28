"""remove source column from isolates"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251028_add_mash_contamination"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "taxonomic_assignments",
        sa.Column("mash_contamination", sa.Float(), nullable=True),
    )
    op.add_column(
        "taxonomic_assignments",
        sa.Column("mash_contaminated_spp", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("taxonomic_assignments", "mash_contamination")
    op.drop_column("taxonomic_assignments", "mash_contaminated_spp")
