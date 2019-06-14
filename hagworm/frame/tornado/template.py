# -*- coding: utf-8 -*-

import tornado.template

from jinja2 import Template, Environment, FileSystemLoader


class FixedTemplate(Template):

    def generate(self, **kwargs):

        return self.render(**kwargs)


class Jinja2Loader(tornado.template.Loader):

    def _create_template(self, name):

        env = Environment(loader=FileSystemLoader(self.root))

        return env.get_template(name)


Environment.template_class = FixedTemplate
