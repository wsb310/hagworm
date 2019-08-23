# -*- coding: utf-8 -*-

from tornado.web import StaticFileHandler

from controller import home


router = [

    (r'/static/(.*)', StaticFileHandler),

    (r'/?', home.Default),

    (r'/download/?', home.Download),

    (r'/event/?', home.Event),

    (r'/socket/(\w+)/?', home.Socket),

]
