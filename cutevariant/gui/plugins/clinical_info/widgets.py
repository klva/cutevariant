from PySide2.QtCore import Qt, Slot
from PySide2.QtWidgets import (
    QWidget,
    QTextEdit,
    QLineEdit,
    QVBoxLayout,
    QFormLayout,
    QScrollArea,
    QSizePolicy,
    QLabel,
)
from cutevariant.gui.plugin import PluginWidget
from cutevariant.gui.plugins.clinical_info.sql import get_clinical_info


class ClinicalInfoWidget(PluginWidget):
    """
        Widget to display arbitrary clinical data from the database
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        label = QLabel("No project")
        self.scroll_area.setWidget(label)
        layout.addWidget(self.scroll_area)
        self.setLayout(layout)

    def create_widget(self, display_type, value):
        if display_type == "shorttext":
            widget = QLineEdit()
            widget.setReadOnly(True)
            widget.setText(value)
        elif display_type == "longtext":
            widget = QTextEdit()
            widget.setReadOnly(True)
            widget.setPlainText(value)
            widget.setFixedHeight(110)
        else:
            # TODO maybe do something better than that
            widget = QLabel(f"Unsupported field type {display_type} !")

        return widget

    def on_open_project(self, conn):
        print("open project")
        clinical_info = get_clinical_info(conn)
        print(clinical_info)
        if not clinical_info:
            print("non")
            # Table is absent or empty
            label = QLabel("No clinical information in this project")
            self.scroll_area.setWidget(label)
        else:
            print("oui")
            scroll_widget = QWidget()
            scroll_widget.setSizePolicy(
                QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed
            )
            scroll_layout = QFormLayout()
            scroll_layout.setRowWrapPolicy(QFormLayout.WrapAllRows)
            for field in clinical_info:
                widget = self.create_widget(field["display_type"], field["value"])
                scroll_layout.addRow(f"<b>{field['name']}</b>", widget)

            scroll_widget.setLayout(scroll_layout)
            self.scroll_area.setWidget(scroll_widget)
