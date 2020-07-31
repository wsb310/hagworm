# -*- coding: utf-8 -*-

import qrcode

from io import BytesIO
from qrcode.image.svg import SvgPathImage


class QRCode:

    @staticmethod
    def make(data):

        code_img = BytesIO()

        qrcode.make(data).save(code_img)

        return code_img.getvalue()

    @staticmethod
    def make_svg(data):

        code_img = BytesIO()

        qrcode.make(data, image_factory=SvgPathImage).save(code_img)

        return code_img.getvalue().decode()
