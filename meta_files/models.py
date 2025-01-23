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
class Id:
    def __init__(self, *args, **kwargs):
        pass

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)


class Name:
    def __init__(self, *args, **kwargs):
        pass

    name = Column(String(128), primary_key=True, nullable=False, unique=True)


class AutoName:
    def __init__(self, *args, **kwargs):
        pass

    name = Column(String(128), primary_key=True, nullable=False, unique=True)


class Time:
    def __init__(self, *args, **kwargs):
        pass

    date = Column(DateTime, nullable=True)


class TimeRange:
    def __init__(self, *args, **kwargs):
        pass

    st_date = Column(DateTime, nullable=True)
    exp_date = Column(DateTime, nullable=True)
    ed_date = Column(DateTime, nullable=True)
    ed_date_sure = Column(DateTime, nullable=True)


class Notional:
    def __init__(self, *args, **kwargs):
        pass

    notional = Column(DECIMAL(22, 2), nullable=False)


class TimeStamp:
    def __init__(self, *args, **kwargs):
        pass

    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())


class UserStamp:
    def __init__(self, *args, **kwargs):
        pass

    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class Comment:
    def __init__(self, *args, **kwargs):
        pass

    comment = Column(String(1000), nullable=True)


class Stash:
    def __init__(self, *args, **kwargs):
        pass

    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)


class TableBase(Id, TimeStamp, UserStamp, Comment, Stash):


    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class NameWithNick(Name):


    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    nick = Column(String(128), nullable=True, unique=True)


class Contact(Base, TableBase, NameWithNick):

    __tablename__ = 'contact'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    nick = Column(String(128), nullable=True, unique=True)
    cell = Column(String(100), nullable=True)
    email = Column(String(200), nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class Bd(Base, TableBase):

    __tablename__ = 'bd'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    contact_name = Column(String(128), ForeignKey(f'mint_data.contact.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    bd_leader_name = Column(String(128), ForeignKey(f'mint_data.bd_leader.contact_name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class BdLeader(Base, TableBase):

    __tablename__ = 'bd_leader'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    contact_name = Column(String(128), ForeignKey(f'mint_data.contact.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    location = Column(String(50), nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class BankAccount(Base, TableBase, AutoName):

    __tablename__ = 'bank_account'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    account_name = Column(String(200), nullable=False)
    account_number = Column(String(200), nullable=False)
    bank_full_name = Column(String(200), nullable=False)
    bank_province = Column(String(50), nullable=True)
    bank_city = Column(String(50), nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class Credit(Base, TableBase, Name, TimeRange, Notional):

    __tablename__ = 'credit'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    st_date = Column(DateTime, nullable=True)
    exp_date = Column(DateTime, nullable=True)
    ed_date = Column(DateTime, nullable=True)
    ed_date_sure = Column(DateTime, nullable=True)
    notional = Column(DECIMAL(22, 2), nullable=False)
    type_assets = Column(String(50), nullable=True)
    type_credit = Column(String(50), nullable=True)
    type_project = Column(String(50), nullable=True)
    type_use = Column(String(200), nullable=False)
    currency = Column(String(50), nullable=True, default='人民币', server_default='人民币')
    status = Column(String(50), nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class Inst(Base, TableBase, NameWithNick):

    __tablename__ = 'inst'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    nick = Column(String(128), nullable=True, unique=True)
    hq_name = Column(String(50), nullable=True)
    branch_name = Column(String(50), nullable=True)
    cover_bd_name = Column(String(128), ForeignKey(f'mint_data.bd.contact_name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    type = Column(String(50), nullable=True)
    is_dxm_group = Column(Integer, nullable=False, default=0.0, server_default='0')
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectTypeLedger(Base, TableBase):

    __tablename__ = 'project_type_ledger'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    type_ledger = Column(String(50), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectTypeLedgerDetail(Base, TableBase):

    __tablename__ = 'project_type_ledger_detail'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    type_ledger_detail = Column(String(50), primary_key=True, nullable=False, unique=True)
    type_ledger = Column(String(50), ForeignKey(f'mint_data.project_type_ledger.type_ledger', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectTypePredict(Base, TableBase):

    __tablename__ = 'project_type_predict'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    type_predict = Column(String(50), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectTypeScene(Base, TableBase):

    __tablename__ = 'project_type_scene'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    type_scene = Column(String(50), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevelType(Base, TableBase):

    __tablename__ = 'project_level_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    level_type = Column(String(128), primary_key=True, nullable=False, unique=True)
    is_priority = Column(Integer, nullable=False, default=1.0, server_default='1')
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevelPrincipalRepayMethod(Base, TableBase):

    __tablename__ = 'project_level_principal_repay_method'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    principal_repay_method = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevelInterestsRepayMethod(Base, TableBase):

    __tablename__ = 'project_level_interests_repay_method'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    interests_repay_method = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevelInterestsRepayDay(Base, TableBase):

    __tablename__ = 'project_level_interests_repay_day'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    interests_repay_day = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectInstType(Base, TableBase):

    __tablename__ = 'project_inst_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    inst_type = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectFeeBase(Base, TableBase):

    __tablename__ = 'project_fee_base'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    fee_base = Column(String(128), primary_key=True, nullable=False, unique=True)
    is_project_base = Column(Integer, nullable=True)
    is_priority_base = Column(Integer, nullable=True)
    is_establish_base = Column(Integer, nullable=True)
    is_balance_base = Column(Integer, nullable=True)
    is_fixed_amount = Column(Integer, nullable=True)
    is_fixed_base = Column(Integer, nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectFeeType(Base, TableBase):

    __tablename__ = 'project_fee_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    fee_type = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectAccountType(Base, TableBase):

    __tablename__ = 'project_account_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    account_type = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class Project(Base, TableBase, NameWithNick, TimeRange, Notional):

    __tablename__ = 'project'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    nick = Column(String(128), nullable=True, unique=True)
    st_date = Column(DateTime, nullable=True)
    exp_date = Column(DateTime, nullable=True)
    ed_date = Column(DateTime, nullable=True)
    ed_date_sure = Column(DateTime, nullable=True)
    notional = Column(DECIMAL(22, 2), nullable=False)
    rd_id = Column(String(128), nullable=True, unique=True)
    annual_days = Column(Integer, nullable=False, default=365.0)
    actual_exp_date = Column(DateTime, nullable=True)
    cost_st_date = Column(DateTime, nullable=True)
    type_ledger_detail = Column(String(50), ForeignKey(f'mint_data.project_type_ledger_detail.type_ledger_detail', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    type_predict = Column(String(50), ForeignKey(f'mint_data.project_type_predict.type_predict', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    type_scene = Column(String(50), ForeignKey(f'mint_data.project_type_scene.type_scene', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    credit_name = Column(String(128), ForeignKey(f'mint_data.credit.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    off_balance_sheet_type = Column(String(50), ForeignKey(f'mint_data.project_off_balance_sheet_type.off_balance_sheet_type', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default=0.0, server_default='0')
    fpt_pushing_time = Column(DateTime, nullable=True)
    is_fed_project = Column(Integer, nullable=False, default=1.0, server_default='1')
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevel(Base, TableBase, AutoName):

    __tablename__ = 'project_level'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    project_name = Column(String(128), ForeignKey(f'mint_data.project.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    order_id = Column(Integer, nullable=True)
    level_type = Column(String(100), ForeignKey(f'mint_data.project_level_type.level_type', ondelete='NO ACTION', onupdate='CASCADE'), nullable=True)
    weights = Column(Float, nullable=False)
    rate = Column(Float, nullable=False)
    ranking = Column(String(100), nullable=True)
    funding_type = Column(String(100), nullable=True)
    face_rate = Column(Float, nullable=True)
    face_weight = Column(Float, nullable=True)
    is_float_rate = Column(Integer, nullable=False, default=0.0, server_default='0')
    float_rate_type = Column(String(128), nullable=True, default='不适用', server_default='不适用')
    principal_repay_method = Column(String(128), ForeignKey(f'mint_data.project_level_principal_repay_method.principal_repay_method', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='不适用', server_default='不适用')
    interests_repay_method = Column(String(128), ForeignKey(f'mint_data.project_level_interests_repay_method.interests_repay_method', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='不适用', server_default='不适用')
    interests_repay_day = Column(String(128), ForeignKey(f'mint_data.project_level_interests_repay_day.interests_repay_day', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='不适用', server_default='不适用')
    rate_adjust_method = Column(String(128), nullable=True, default='不适用', server_default='不适用')
    rate_adjust_freq = Column(String(128), ForeignKey(f'mint_data.project_level_rate_adjust_freq.rate_adjust_freq', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='不适用', server_default='不适用')
    rate_adjust_day = Column(String(128), ForeignKey(f'mint_data.project_level_rate_adjust_day.rate_adjust_day', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='不适用', server_default='不适用')
    level_predict_exp_date = Column(DateTime, nullable=True)
    investor_name = Column(String(128), ForeignKey(f'mint_data.inst.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    is_funding_targeted = Column(Integer, nullable=False, default=1.0, server_default='1')
    is_cal_profit = Column(Integer, nullable=False, default=1.0, server_default='1')
    is_credit_occupied = Column(Integer, nullable=False, default=1.0, server_default='1')
    is_face_rate = Column(Integer, nullable=False, default=1.0, server_default='1')
    is_init = Column(Integer, nullable=True, default=1.0, server_default='1')
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevelInterestsRepay(Base, TableBase):

    __tablename__ = 'project_level_interests_repay'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    project_level_name = Column(String(128), ForeignKey(f'mint_data.project_level.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    date = Column(DateTime, nullable=False)
    amount = Column(DECIMAL(22, 2), nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectChange(Base, TableBase):

    __tablename__ = 'project_change'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    project_level_name = Column(String(128), ForeignKey(f'mint_data.project_level.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    change_date = Column(DateTime, nullable=True)
    notional_delta = Column(DECIMAL(22, 2), nullable=True)
    rate_delta = Column(Float, nullable=True)
    notional_to = Column(DECIMAL(22, 2), nullable=True)
    rate_to = Column(Float, nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectFee(Base, TableBase):

    __tablename__ = 'project_fee'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    project_name = Column(String(128), ForeignKey(f'mint_data.project.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    auth_date = Column(DateTime, nullable=True)
    exe_date = Column(DateTime, nullable=True)
    predict_date = Column(DateTime, nullable=True)
    fee_amount = Column(Float, nullable=True)
    fee_type = Column(String(128), ForeignKey(f'mint_data.project_fee_type.fee_type', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    annual_days = Column(Integer, nullable=True)
    fee_payer = Column(String(128), ForeignKey(f'mint_data.inst.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    fee_collector = Column(String(128), ForeignKey(f'mint_data.inst.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    fee_base = Column(String(128), ForeignKey(f'mint_data.project_fee_base.fee_base', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    fee_base_amount = Column(DECIMAL(22, 2), nullable=True)
    amt_way_cost = Column(String(128), ForeignKey(f'mint_data.project_fee_amt_way_type.amt_way', ondelete='NO ACTION', onupdate='CASCADE'), nullable=True)
    amt_way_balance = Column(String(128), ForeignKey(f'mint_data.project_fee_amt_way_type.amt_way', ondelete='NO ACTION', onupdate='CASCADE'), nullable=True, default='自成立摊销一年', server_default='自成立摊销一年')
    is_cal_profit = Column(Integer, nullable=False, default=1.0, server_default='1')
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectInst(Base, TableBase):

    __tablename__ = 'project_inst'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    project_name = Column(String(128), ForeignKey(f'mint_data.project.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    inst_type = Column(String(128), ForeignKey(f'mint_data.project_inst_type.inst_type', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    inst_name = Column(String(128), ForeignKey(f'mint_data.inst.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    flow_type = Column(String(128), nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectAccount(Base, TableBase):

    __tablename__ = 'project_account'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    project_name = Column(String(128), ForeignKey(f'mint_data.project.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    account_type = Column(String(128), ForeignKey(f'mint_data.project_account_type.account_type', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    bank_account_name = Column(String(128), ForeignKey(f'mint_data.bank_account.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectAttachment(Base, TableBase):

    __tablename__ = 'project_attachment'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    project_name = Column(String(128), ForeignKey(f'mint_data.project.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    contract_type = Column(String(128), ForeignKey(f'mint_data.project_contract_type.contract_type', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    contract_erp_id = Column(String(200), nullable=False)
    file_remote_path = Column(String(300), nullable=True)
    file_data_port = Column(BLOB(1000), nullable=True)
    md5 = Column(String(256), nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectStructuredInfo(Base, TableBase):

    __tablename__ = 'project_structured_info'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    project_name = Column(String(128), ForeignKey(f'mint_data.project.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, unique=True)
    trustee_project_id_str = Column(String(50), nullable=False)
    inner_credit_enhancement_inst = Column(String(128), ForeignKey(f'mint_data.inst.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    inner_credit_enhancement_method = Column(TEXT, nullable=False)
    external_credit_enhancement_inst = Column(String(128), ForeignKey(f'mint_data.inst.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    external_credit_enhancement_method = Column(TEXT, nullable=False)
    ryc_freq = Column(String(10), ForeignKey(f'mint_data.project_structured_info_recycle_freq_type.ryc_freq', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    exp_date_ryc = Column(DateTime, nullable=True)
    st_date_amortize = Column(DateTime, nullable=True)
    clear_buyback_date = Column(DateTime, nullable=True)
    repayment_date_string = Column(String(50), nullable=True)
    structure = Column(String(10), nullable=True)
    ryc_periods = Column(Integer, nullable=True)
    amt_periods = Column(Integer, nullable=True)
    amt_tag = Column(String(128), ForeignKey(f'mint_data.project_amt_tag.amt_tag', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='不适用', server_default='不适用')
    idle_rate_threshold = Column(Float, nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectBd(Base, TableBase):

    __tablename__ = 'project_bd'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    project_name = Column(String(128), ForeignKey(f'mint_data.project.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    cover_bd_name = Column(String(128), ForeignKey(f'mint_data.bd.contact_name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False, default='未指定', server_default='未指定')
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectStructuredInfoRecycleFreqType(Base, TableBase):

    __tablename__ = 'project_structured_info_recycle_freq_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    ryc_freq = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectOffBalanceSheetType(Base, TableBase):

    __tablename__ = 'project_off_balance_sheet_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    off_balance_sheet_type = Column(String(50), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectFeeAmtWayType(Base, TableBase):

    __tablename__ = 'project_fee_amt_way_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    amt_way = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevelFloatRateType(Base, TableBase):

    __tablename__ = 'project_level_float_rate_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    float_rate_type = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevelRateAdjustFreq(Base, TableBase):

    __tablename__ = 'project_level_rate_adjust_freq'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    rate_adjust_freq = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectLevelRateAdjustDay(Base, TableBase):

    __tablename__ = 'project_level_rate_adjust_day'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    rate_adjust_day = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectPredictParam(Base, TableBase):

    __tablename__ = 'project_predict_param'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    group_name = Column(String(128), nullable=False)
    type_predict = Column(String(50), ForeignKey(f'mint_data.project_type_predict.type_predict', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    st_date = Column(DateTime, nullable=False)
    notional = Column(DECIMAL(22, 2), nullable=False)
    avg_rate = Column(Float, nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectAmtTag(Base, TableBase):

    __tablename__ = 'project_amt_tag'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    amt_tag = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class AmtParam(Base, TableBase):

    __tablename__ = 'amt_param'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    amt_tag = Column(String(128), ForeignKey(f'mint_data.project_amt_tag.amt_tag', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    step = Column(Integer, nullable=False)
    amt_rate = Column(Float, nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class ProjectContractType(Base, TableBase):

    __tablename__ = 'project_contract_type'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    contract_type = Column(String(128), primary_key=True, nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class Calendar(Base, TableBase):

    __tablename__ = 'calendar'
    __table_args__ = {'schema': 'mint_data', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    date = Column(Date, nullable=False)
    is_trading_day = Column(Integer, nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class Users(Base, TableBase, Name):

    __tablename__ = 'users'
    __table_args__ = {'schema': 'mint_admin', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    status = Column(String(128), nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class Character(Base, TableBase, Name):

    __tablename__ = 'character'
    __table_args__ = {'schema': 'mint_admin', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    label = Column(String(128), nullable=False, unique=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class WebResources(Base, TableBase, Name):

    __tablename__ = 'web_resources'
    __table_args__ = {'schema': 'mint_admin', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(128), primary_key=True, nullable=False, unique=True)
    block = Column(String(128), nullable=True)
    title = Column(String(128), nullable=False, unique=True)
    to = Column(String(256), nullable=True)
    icon = Column(String(128), nullable=True)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class UserCharacter(Base, TableBase):

    __tablename__ = 'user_character'
    __table_args__ = {'schema': 'mint_admin', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    username = Column(String(128), ForeignKey(f'mint_admin.users.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    character = Column(String(128), ForeignKey(f'mint_admin.character.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class CharacterResources(Base, TableBase):

    __tablename__ = 'character_resources'
    __table_args__ = {'schema': 'mint_admin', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    character = Column(String(128), ForeignKey(f'mint_admin.character.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    web_resource = Column(String(128), ForeignKey(f'mint_admin.web_resources.name', ondelete='NO ACTION', onupdate='CASCADE'), nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class PageWidgets(Base, TableBase):

    __tablename__ = 'page_widgets'
    __table_args__ = {'schema': 'mint_admin', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    path = Column(String(256), nullable=False)
    api = Column(String(256), nullable=False)
    title = Column(String(128), nullable=False)
    tooltip = Column(String(128), nullable=True)
    type = Column(String(128), nullable=False)
    grid_pos = Column(String(128), nullable=False)
    comment = Column(String(1000), nullable=True)
    stashed = Column(Integer, nullable=True)
    stash_date = Column(DateTime, nullable=True)
    stash_comments = Column(String(128), nullable=True)
    create_time = Column(DateTime, nullable=True, server_default=func.now())
    update_time = Column(DateTime, nullable=True, server_default=func.now(), onupdate=func.now())
    create_user = Column(String(100), nullable=True)
    update_user = Column(String(100), nullable=True)


class DmFinZiyingIssuePayableLessDd(Base, Id):

    __tablename__ = 'dm_fin_ziying_issue_payable_less_dd'
    __table_args__ = {'schema': 'mint_mdw_dm', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    stat_dt = Column(String(20), nullable=False)
    sec_business_tag = Column(String(100), nullable=True)
    zy_principal_payable = Column(DOUBLE, nullable=True)


class DashboardWidgetOptions(Base, Id):

    __tablename__ = 'dashboard_widget_options'
    __table_args__ = {'schema': 'mint_admin', 'comment': ''}

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    dashboard = Column(String(128), nullable=True)
    url = Column(String(128), nullable=False)
    title = Column(String(128), nullable=True)
    col_span = Column(Integer, nullable=False)
    row_span = Column(Integer, nullable=False)


# table class end
