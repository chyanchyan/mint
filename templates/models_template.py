# coding: utf-8
from sqlalchemy import Integer, Column, Date, DateTime, \
    Float, ForeignKey, String, TEXT, func, BLOB, DECIMAL, DOUBLE
from sqlalchemy.orm import declarative_base
if 'mint' in __name__.split('.'):
    from ..sys_init import *
else:
    from mint.sys_init import *


Base = declarative_base()


# table class start

# table class end
