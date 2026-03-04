from __future__ import annotations

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.database import get_db


def db_session(db: Session = Depends(get_db)) -> Session:
    return db

