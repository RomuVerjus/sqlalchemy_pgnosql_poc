from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, date, time
from decimal import Decimal
from enum import Enum
from types import GeneratorType
from typing import List
from uuid import uuid4

from pydantic import BaseModel
from sqlalchemy import create_engine, Column
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.engine import RowProxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


def isoformat(o):
    return o.isoformat()


class CustomEncoder(json.JSONEncoder):
    ENCODER_BY_TYPE = {
        uuid.UUID: str,
        datetime: isoformat,
        date: isoformat,
        time: isoformat,
        set: list,
        frozenset: list,
        GeneratorType: list,
        bytes: lambda o: o.decode(),
        Decimal: str,
        RowProxy: dict,
    }

    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        try:
            encoder = self.ENCODER_BY_TYPE[type(obj)]
        except KeyError:
            return super().default(obj)
        return encoder(obj)


def dumps(d):
    return json.dumps(d, cls=CustomEncoder)


Base = declarative_base()


class Tracing(Base):
    __tablename__ = "yarrow"
    id = Column('id', UUID(as_uuid=True), primary_key=True, default=uuid4)
    data = Column('data', JSONB)


class Truc(BaseModel):
    machin: str


class RadladTracingMode(BaseModel):
    master_uuid: uuid.UUID
    uuid: uuid.UUID
    config_name: str
    machins: List[Truc] = []

    def update(self, new_tracing: RadladTracingMode):
        self.machins += new_tracing.machins


engine = create_engine(URL_DB, echo=True, json_serializer=dumps)
session_makers = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def insert_data(data: RadladTracingMode, my_session: Session):
    truc_to_add = Tracing(data=data.dict())
    my_session.add(truc_to_add)
    my_session.commit()
    my_session.refresh(truc_to_add)
    return truc_to_add


def does_exist(data: RadladTracingMode, my_session: Session):
    return my_session.query(Tracing).filter(Tracing.data.contains({"master_uuid": str(data.master_uuid)})).first()


identifiers = RadladTracingMode(**{
    "master_uuid": uuid.UUID("dbd39cef-cf84-4ddb-bda4-3169e74f8774"),
    "uuid": uuid4(),
    "config_name": "test"
})

new_results = RadladTracingMode(**{
    "master_uuid": uuid.UUID("dbd39cef-cf84-4ddb-bda4-3169e74f8774"),
    "uuid": uuid4(),
    "config_name": "test",
    "machins": [Truc(**{"machin": "bidule"})]
})

session: Session = session_makers()
session.execute('SET search_path TO tracing')

old_truc = does_exist(new_results, session)

if old_truc:
    tracing = RadladTracingMode(**old_truc.data)
    tracing.update(new_results)
    tracing_updated = Tracing(data=tracing.dict(), id=old_truc.id)
    session.merge(tracing_updated)
    session.commit()
else:
    insert_data(identifiers, session)

session.close()
engine.dispose()
