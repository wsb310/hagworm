# -*- coding: utf-8 -*-

from controller import home


router = [

    (r'/?', home.Default),

    (r'/download/?', home.Download),

    (r'/socket/(\w+)/?', home.Socket),

]
