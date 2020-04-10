import functools

from PySide2.QtGui import QColor, QFont, QIcon

from cutevariant.gui import FIcon, style
from cutevariant.gui.formatters.default import DefaultFormatter


class AuragenFormatter(DefaultFormatter):

    VARIANT_TAG_COLORS = {
        "inherited_parental_lineage_1": "#33984b",
        "possible_inherited_parental_lineage_1": "#99e65f",
        "inherited_parental_lineage_2": "#0069aa",
        "possible_inherited_parental_lineage_2": "#00cdf9",
        "de_novo": "#ff5000",
        "possible_de_novo": "#ffc825",
        "homozygous": "#db3ffd",
        "complex": "#7a09fa",
    }
    DEFAULT_VARIANT_TAG_COLOR = "#3d3d3d"

    def __init__(self):
        return super().__init__()

    @functools.lru_cache(maxsize=128)
    def get_font(self, column, value):
        font = QFont()
        font.setBold(True)
        return font

    @functools.lru_cache(maxsize=128)
    def get_foreground(self, column, value):
        if column == "variant_tag":
            return QColor(
                self.VARIANT_TAG_COLORS.get(value, self.DEFAULT_VARIANT_TAG_COLOR)
            )

        return super().get_foreground(column, value)

    def get_decoration(self, column, value):
        res = super().get_decoration(column, value)
        if res:
            return res

        if column in (
            "gene_in_expert_panel",
            "gene_in_hpo_panel",
            "clinvar_patho",
            "in_tightlist",
            "in_roh",
        ) or column.startswith("panel_"):
            # TODO generalize that to all bool fields
            if value == "True":
                # Green tick
                return QIcon(FIcon(0xF12C, style.DARK_COLOR["green"]))
            else:
                # Red cross
                return QIcon(FIcon(0xF156, style.DARK_COLOR["red"]))

        return None
