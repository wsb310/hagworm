# -*- coding: utf-8 -*-

import copy

from io import BytesIO
from datetime import datetime

from xlwt import Workbook, XFStyle, Borders, Pattern


class ExcelWT(Workbook):
    """Excel生成工具
    """

    def __init__(self, name, encoding=r'utf-8', style_compression=0):

        super().__init__(encoding, style_compression)

        self._book_name = name
        self._current_sheet = None

        self._default_style = XFStyle()
        self._default_style.borders.left = Borders.THIN
        self._default_style.borders.right = Borders.THIN
        self._default_style.borders.top = Borders.THIN
        self._default_style.borders.bottom = Borders.THIN
        self._default_style.pattern.pattern = Pattern.SOLID_PATTERN
        self._default_style.pattern.pattern_fore_colour = 0x01

        self._default_title_style = copy.deepcopy(self._default_style)
        self._default_title_style.font.bold = True
        self._default_title_style.pattern.pattern_fore_colour = 0x16

    def create_sheet(self, name, titles=[]):

        sheet = self._current_sheet = self.add_sheet(name)
        style = self._default_title_style

        for index, title in enumerate(titles):
            sheet.write(0, index, title, style)
            sheet.col(index).width = 0x1200

    def add_sheet_row(self, *args):

        sheet = self._current_sheet
        style = self._default_style

        nrow = len(sheet.rows)

        for index, value in enumerate(args):
            sheet.write(nrow, index, value, style)

    def get_file(self):

        result = b''

        with BytesIO() as stream:

            self.save(stream)

            result = stream.getvalue()

        return result

    def write_request(self, request):

        filename = f"{self._book_name}.{datetime.today().strftime('%y%m%d.%H%M%S')}.xls"

        request.set_header(r'Content-Type', r'application/vnd.ms-excel')
        request.set_header(r'Content-Disposition', f'attachment;filename={filename}')

        return request.finish(self.get_file())
