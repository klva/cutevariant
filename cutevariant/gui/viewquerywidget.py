# Standard imports
import copy
import csv

# Qt imports
from PySide2.QtWidgets import *
from PySide2.QtCore import *
from PySide2.QtGui import *

# Custom imports
from cutevariant.gui.ficon import FIcon
from .plugin import QueryPluginWidget
from cutevariant.core import Query
from cutevariant.core import sql
from cutevariant.gui.style import IMPACT_COLOR
from cutevariant.commons import logger

LOGGER = logger()


class QueryModel(QAbstractItemModel):
    """
    QueryModel is the base class to display variant in the view. 
    It can load paginated data using limit and page attribute.
    
    Attributes:
        limit(int): Variant count to display. See SQL LIMIT 
        page(int): current page to display. See SQL OFFSET 
        total(int): Total variant count 
        variants(list): Internal data to store variants

    """


    def __init__(self, parent=None):
        super().__init__()
        self.limit = 50
        self.page = 0
        self.total = 0
        self._query = None
        self.variants = []  
        



    def is_root(self, index: QModelIndex) -> bool:
        """
        Return True if the parent of index is the invisible root index 
        """
        return index == QModelIndex()


    # def row_id(self, index: QModelIndex):
    #     """
    #     Return rowid of the variant identified by index 

    #     :return: -1 if not avaible 
    #     """
    #     if not self.variants or not self.hasIndex(index):
    #         return -1 

    #     return int(self.variants[index.row()][0])



    def rowCount(self, parent=QModelIndex()):
        """
        Overrided : Return model row count
        """
        # If parent is root 
        
        if parent == QModelIndex():
            return len(self.variants)

        if parent.parent() == QModelIndex():
            return len(self.variants[parent.row()])

        # Get parent variant row ID and return child count 
        # if self.row_id(parent) in self.childs:
        #     return len(self.childs[parent.row()])
        # else:
        #return 0

    def columnCount(self, parent=QModelIndex()):
        """Overrided: Return column count """

        # If no query is defined
        if not self._query:
            return 0

        return len(self._query.columns) + 1  # show child count for the first col

    def index(self, row, column, parent=QModelIndex()):
        """Overrided: Return index """

        if not self.hasIndex(row, column, parent):
            return QModelIndex()

        if parent == QModelIndex():
            #row_id = int(self.variants[row][0])
            return self.createIndex(row, column, None)  

        if parent.parent() == QModelIndex():
            return self.createIndex(row, column, parent.row())

    def parent(self, child):
        """Overrided: Return parent of child """

        if not child.isValid():
            return QModelIndex()

        if child.internalId() == None:
            return QModelIndex()

        else:
            return self.index(child.internalId(),0, QModelIndex())


    def data(self, index, role=Qt.DisplayRole):
        """ Overrided: return value of index according role  """

        if not index.isValid():
            return None

        # Display Role 
        if role == Qt.DisplayRole:
            if index.parent() == QModelIndex():
                if index.column() == 0:
                    return str(self.variants[index.row()][0][-1])
                else:
                    return str(self.variants[index.row()][0][index.column()]) 

            else:
                if index.column() == 0:
                    return None
                else:
                    return str(self.variants[index.parent().row()][index.row()][index.column() + 1])

        # Icon role : show number of child 
        if role == Qt.DecorationRole:
            if index.parent() == QModelIndex() and index.column() == 0 and self.hasChildren(index):
                return self._draw_child_count_icon(self.variants[index.row()][0][-1])


        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        """Overrided: Return column name as header data """
        if orientation == Qt.Horizontal:
            if role == Qt.DisplayRole:
                if section == 0:
                    return "childs"
                else:
                    return self._query.columns[section-1]

        if orientation == Qt.Vertical:
            if role == Qt.DisplayRole:
                return self.variants[index.row()][-1]

        return None

    def hasChildren(self, parent: QModelIndex) -> bool:
        #return False
        """Overrided: Return True if parent has children """
        # if invisible root node, always return True
        if parent == QModelIndex():
            return True

        if parent.parent() == QModelIndex():
            childs_count = self.variants[parent.row()][0][-1]
            return childs_count > 1

        return False
        

        # if parent.parent() == QModelIndex():
        #     return self._child_count(parent) > 1

    def canFetchMore(self, parent: QModelIndex) -> bool:
        """ Overrided """
        return self.hasChildren(parent)

    def fetchMore(self, parent: QModelIndex):
        """Overrided """
        if parent == QModelIndex():
            return

        variant_id = self.variants[parent.row()][0][0]
        columns = ",".join(self._query.columns)

        # TODO : need to put this into QUERY
        sub_query = (f"""SELECT variants.rowid,{columns} FROM variants 
        LEFT JOIN annotations ON variants.rowid = annotations.variant_id 
        WHERE variants.rowid = {variant_id}""")

        records = list(self._query.conn.cursor().execute(sub_query).fetchall())

        self.beginInsertRows(parent, 0, len(records) - 1)

        # Clear pevious childs 
        self.variants[parent.row()][1:] = []

        for idx, record in enumerate(records):
            #if idx != 0: # Don't add the first one... it's the parent 
            self.variants[parent.row()].append(tuple(record))
            

        # self.childs[parent.row()] = []
        # self.childs[parent.row()] = list(child_query.rows())


        self.endInsertRows()



    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query: Query):
        self._query = query
        # self._query.group_by=("chr","pos","ref","alt")


    def load(self):
        """
        Load variant data into the model from query attributes

        """
        self._query.group_by = ("chr","pos")
        self.beginResetModel()
        self.variants = []
        self.total = self.query.count()


        for variant in self._query.rows(self.limit, self.page * self.limit):
            self.variants.append([tuple(variant)])  # Append a list because child can be append after 
        
        LOGGER.debug("QueryModel:load:: variants queried\n%s", self.variants)
        self.endResetModel()

    def sort(self, column: int, order):
        """Overrided"""
        if column < self.columnCount():
            colname = self._query.columns[column]

            print("ORDER", order)
            self._query.order_by = colname
            self._query.order_desc = order == Qt.DescendingOrder
            self.load()

    def hasPage(self, page):
        return page >= 0 and page * self.limit < self.total

    def setPage(self, page):
        if self.hasPage(page):
            self.page = page
            self.load()

    def displayed(self):
        """Get ids of first, last displayed variants on the total number

        :return: Tuple with (first_id, last_id, self.total).
        :rtype: <tuple <int>,<int>,<int>>
        """
        first_id = self.limit * self.page

        if self.hasPage(self.page + 1):
            # Remainder : self.total - (self.limit * (self.page + 1)))
            last_id = self.limit * (self.page + 1)
        else:
            # Remainder : self.total - (self.limit * self.page)
            last_id = self.total

        return (first_id, last_id, self.total)

    def nextPage(self):
        """Display the next page of data if it exists"""
        if self.hasPage(self.page + 1):
            self.setPage(self.page + 1)

    def previousPage(self):
        """Display the previous page of data if it exists"""
        if self.hasPage(self.page - 1):
            self.setPage(self.page - 1)

    def variant(self, index: QModelIndex):

        if index.parent() == QModelIndex():
            return self.variants[index.row()][0]

        if index.parent().parent() == QModelIndex():
            return self.variants[index.parent().row()][index.row()]

    def _draw_child_count_icon(self, count : int) -> QIcon :

        pix = QPixmap(48,41)
        pix.fill(Qt.transparent)
        painter = QPainter()
        painter.begin(pix)
        painter.setBrush(QColor("#f1646c"))
        painter.setPen(Qt.NoPen)
        painter.drawRoundedRect(pix.rect().adjusted(1,1,-1,-1), 10, 10)
        font = QFont()
        font.setBold(True)
        font.setPixelSize(25)
        painter.setFont(font)
        painter.setPen(Qt.white)
        painter.drawText(pix.rect(), Qt.AlignCenter, str(count))

        painter.end()


        return QIcon(pix)



class QueryDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        """overriden"""

        palette = qApp.palette("QTreeView")
        colname = index.model().headerData(index.column(), Qt.Horizontal)
        value = index.data(Qt.DisplayRole)
        select = option.state & QStyle.State_Selected

        if not select:
            bg_brush = option.backgroundBrush
        else:
            bg_brush = palette.brush(QPalette.Highlight)


        painter.setBrush(bg_brush)
        painter.setPen(Qt.NoPen)
        painter.drawRect(option.rect)

        alignement = Qt.AlignLeft|Qt.AlignVCenter

        # # Add margin for first columns if index is first level 
        # if index.column() == 0 and index.parent() == QModelIndex():

        #     expanded = bool(option.state & QStyle.State_Open)

        #     branch_option = copy.copy(option)
        #     branch_option.rect.setWidth(65)

        #     qApp.style().drawPrimitive(QStyle.PE_IndicatorBranch, branch_option, painter)

        #     icon = index.data(Qt.DecorationRole)
        #     if icon:
        #         target = QRect(0,0, option.decorationSize.width(), option.decorationSize.height())
        #         target.moveCenter(option.rect.center())
        #         painter.drawPixmap(option.rect.x()+5, target.top() ,icon.pixmap(option.decorationSize))
            
        # if index.column() == 0:    
        #     option.rect.adjust(40,0,0,0)


        if colname == "impact":
            painter.setPen(QPen(IMPACT_COLOR.get(value, palette.color(QPalette.Text))))
            painter.drawText(option.rect, alignement, index.data())
            return

        painter.setPen(QPen(palette.color(QPalette.HighlightedText if select else QPalette.Text)))
        painter.drawText(option.rect, alignement, index.data())

    def draw_biotype(self, value):
        pass

    def sizeHint(self, option, index):
        """override"""
        return QSize(0, 50)


class ViewQueryWidget(QueryPluginWidget):

    variant_clicked = Signal(dict)

    def __init__(self):
        super().__init__()
        self.model = QueryModel()
        self.delegate = QueryDelegate()
        self.setWindowTitle(self.tr("Variants"))
        self.topbar = QToolBar()
        self.bottombar = QToolBar()
        self.view = QTreeView()

        self.view.setFrameStyle(QFrame.NoFrame)
        self.view.setModel(self.model)
        self.view.setItemDelegate(self.delegate)
        self.view.setAlternatingRowColors(True)
        self.view.setSortingEnabled(True)
        self.view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.view.setSelectionMode(QAbstractItemView.ContiguousSelection)
        self.view.setRootIsDecorated(True) # Manage from delegate
        #self.view.setIndentation(0)
        self.view.setIconSize(QSize(22,22))
        self.view.setAnimated(True)
        self.view.setAnimated(True)

        # self.view.setItemDelegate(self.delegate)

        main_layout = QVBoxLayout()
        main_layout.addWidget(self.topbar)
        main_layout.addWidget(self.view)
        main_layout.addWidget(self.bottombar)
        main_layout.setContentsMargins(0, 0, 0, 0)

        # Construct top bar
        # These actions should be disabled until a query is made (see query setter)
        self.export_csv_action = self.topbar.addAction(
            self.tr("Export variants"), self.export_csv
        )
        self.export_csv_action.setEnabled(False)

        # Construct bottom bar
        # These actions should be disabled until a query is made (see query setter)
        self.page_info = QLabel()
        self.page_box = QLineEdit()
        self.page_box.setReadOnly(True)
        self.page_box.setFrame(QFrame.NoFrame)
        self.page_box.setFixedWidth(20)
        self.page_box.setAlignment(Qt.AlignHCenter)
        self.page_box.setStyleSheet("QWidget{background-color: transparent;}")
        self.page_box.setText("0")
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        # Setup actions
        self.show_sql_action = self.bottombar.addAction(
            FIcon(0xF865), self.tr("See SQL query"), self.show_sql
        )
        self.show_sql_action.setEnabled(False)
        self.bottombar.addWidget(self.page_info)
        self.bottombar.addWidget(spacer)
        self.bottombar.addAction(FIcon(0xF141), "<", self.model.previousPage)
        self.bottombar.addWidget(self.page_box)
        self.bottombar.addAction(FIcon(0xF142), ">", self.model.nextPage)
        self.bottombar.setIconSize(QSize(20, 20))

        self.bottombar.setContentsMargins(0, 0, 0, 0)

        self.setLayout(main_layout)

        self.model.modelReset.connect(self.updateInfo)

        # emit variant when clicked
        self.view.clicked.connect(self._variant_clicked)


    def on_init_query(self):
        """ Overrided """
        self.export_csv_action.setEnabled(True)
        self.show_sql_action.setEnabled(True)
        self.model.query = self.query
        #self.on_change_query()




    def on_change_query(self):
        """ Method override from AbstractQueryWidget"""
        self.model.load()
        self.view.header().setSectionResizeMode(0, QHeaderView.ResizeToContents)

    def updateInfo(self):
        """Update metrics for the current query

        .. note:: Update page_info and page_box.
        """

        # Set text
        self.page_info.setText(f"{self.model.total} variant(s)")
        page_box_text = self.tr("{}-{} of {}").format(*self.model.displayed())
        self.page_box.setText(page_box_text)

        # Adjust page_èbox size to content
        fm = self.page_box.fontMetrics()
        self.page_box.setFixedWidth(fm.boundingRect(page_box_text).width() + 5)

    def _variant_clicked(self, index):
        # print("cicked on ", index)
        rowid = self.model.variant(index)[0]
        variant = sql.get_one_variant(self.model.query.conn, rowid)
        self.variant_clicked.emit(variant)

    def export_csv(self):
        """Export variants displayed in the current view to a CSV file"""
        filepath, filter = QFileDialog.getSaveFileName(
            self,
            self.tr("Export variants of the current view"),
            "view.csv",
            self.tr("CSV (Comma-separated values) (*.csv)"),
        )

        if filepath:
            with open(filepath, "w") as f_d:
                writer = csv.writer(f_d, delimiter=",")
                # Write headers (columns in the query) + variants from the model
                writer.writerow(self.model.query.columns)
                # Remove the sqlite rowid col
                g = (variant[1:] for variant in self.model.variants)
                writer.writerows(g)

    def show_sql(self):
        box = QMessageBox()
        try:
            text = self.model.query.sql()
        except AttributeError:
            text = self.tr("No query to show")

        box.setInformativeText(text)
        box.exec_()


    def contextMenuEvent(self, event : QContextMenuEvent):
        """
        Overrided methods 
        """
        current_index = self.view.currentIndex()
        variant = self.model.variant(current_index)

        menu = QMenu(self)

        # actions Examples :
        menu.addAction(FIcon(0XF4CE), self.tr("Add to Favorite")).setCheckable(True)
        menu.addAction(FIcon(0XF18F),self.tr("Copy genomic location"))

        openMenu = menu.addMenu(self.tr("Open With"))

        # @ysard read settings
        openMenu.addAction("Varsome")
        openMenu.addSeparator()
        openMenu.addAction("Edit ...")

        menu.exec_(event.globalPos())
