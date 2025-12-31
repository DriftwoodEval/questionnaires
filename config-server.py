import json
import logging
import os
from typing import Union, get_args, get_origin

import yaml
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from sqlalchemy import Column, Integer, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from utils.custom_types import Config, FullConfig, Services

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)
DATABASE_URL = "sqlite:///config/config_store.db"
engine = create_engine(DATABASE_URL)
Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class ConfigRecord(Base):
    """Represents the config table in the database."""

    __tablename__ = "config"
    id = Column(Integer, primary_key=True, default=1)
    config_data = Column(Text, nullable=False)


def init_db():
    """Initializes the database and creates the config table."""
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    if db.query(ConfigRecord).count() == 0:
        initial_config_data = json.dumps(yaml.safe_load(open("./config/info.yml")))
        db.add(ConfigRecord(config_data=initial_config_data))
        db.commit()
    db.close()


init_db()
app = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
app.mount("/templates", StaticFiles(directory=TEMPLATES_DIR), name="templates")


def get_db():
    """Returns a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_field_schema(field_name: str, field_info: FieldInfo, field_type):
    """Extract schema information from a Pydantic field."""
    schema = {
        "name": field_name,
        "type": "text",
        "required": field_info.is_required(),
        "description": field_info.description or "",
    }

    # Handle type annotations
    origin = get_origin(field_type)

    # Handle lists
    if origin is list:
        args = get_args(field_type)
        schema["type"] = "list"
        if args:
            inner_type = args[0]
            if (
                hasattr(inner_type, "__name__")
                and "email" in inner_type.__name__.lower()
            ):
                schema["item_type"] = "email"
            else:
                schema["item_type"] = "text"
        return schema

    # Handle dicts
    if origin is dict:
        args = get_args(field_type)
        key_type = args[0]
        value_type = args[1]

        # Check for Dict[str, BaseModel] (e.g., PieceworkCosts)
        if (
            key_type == str
            and isinstance(value_type, type)
            and issubclass(value_type, BaseModel)
        ):
            schema["type"] = "dict_object"
            # Recursively generate the schema for the inner object (e.g., PieceworkCosts)
            schema["item_schema"] = generate_schema_from_model(value_type)
            return schema

        # Check for simple key-value pairs (Dict[str, str] or Dict[str, float])
        if key_type == str:
            schema["type"] = "keyvalue"
            type_str = str(value_type).lower()
            if "email" in type_str:
                schema["value_type"] = "email"
            elif "int" in type_str or "float" in type_str:
                schema["value_type"] = "number"
            else:
                schema["value_type"] = "text"
        else:
            schema["type"] = "json"
        return schema

    if origin is Union:
        args = get_args(field_type)
        if args:
            inner_type = getattr(args[0], "__name__", str(args[0]))
            if "email" in inner_type.lower() or "EmailStr" in str(field_type):
                schema["type"] = "email"
            elif "int" in inner_type.lower():
                schema["type"] = "number"
            elif "float" in inner_type.lower():
                schema["type"] = "number"
                schema["step"] = "0.01"
            elif field_name.lower().endswith("password"):
                schema["type"] = "password"
            elif "bool" in inner_type.lower():
                schema["type"] = "checkbox"
        return schema

    # Handle string types
    type_name = getattr(field_type, "__name__", str(field_type))

    if "email" in type_name.lower() or "EmailStr" in str(field_type):
        schema["type"] = "email"
    elif "int" in type_name.lower():
        schema["type"] = "number"
    elif "float" in type_name.lower():
        schema["type"] = "number"
        schema["step"] = "0.01"
    elif field_name.lower().endswith("password"):
        schema["type"] = "password"
    elif "bool" in type_name.lower():
        schema["type"] = "checkbox"

    # Handle constraints from metadata
    if hasattr(field_info, "metadata"):
        for constraint in field_info.metadata:
            if hasattr(constraint, "max_length"):
                schema["maxlength"] = constraint.max_length
            if hasattr(constraint, "pattern"):
                schema["pattern"] = constraint.pattern

    return schema


def generate_schema_from_model(model_class: type[BaseModel], prefix: str = ""):
    """Generate a JSON schema from a Pydantic model for form generation."""
    schema = {"fields": [], "nested": {}}

    for field_name, field_info in model_class.model_fields.items():
        field_type = field_info.annotation
        full_name = f"{prefix}.{field_name}" if prefix else field_name

        # Check if this is a nested Pydantic model
        origin = get_origin(field_type)

        # Handle BaseModel subclasses
        if isinstance(field_type, type) and issubclass(field_type, BaseModel):
            schema["nested"][field_name] = generate_schema_from_model(
                field_type, full_name
            )
        else:
            field_schema = get_field_schema(field_name, field_info, field_type)
            field_schema["full_name"] = full_name
            schema["fields"].append(field_schema)

    return schema


@app.get("/api/schema")
def get_schema():
    """Returns the schema for the configuration form."""
    try:
        config_schema = generate_schema_from_model(Config, "config")
        services_schema = generate_schema_from_model(Services, "services")

        return {"config": config_schema, "services": services_schema}
    except Exception as e:
        logger.exception(f"Schema generation error")
        raise HTTPException(status_code=500, detail=f"Failed to generate schema: {e}")


@app.get("/api/config")
def get_config(db: Session = Depends(get_db)):
    """Returns the current config from the database."""
    record = db.get(ConfigRecord, 1)
    if not record:
        raise HTTPException(status_code=404, detail="Config not found")
    return json.loads(record.config_data)  # type: ignore


@app.post("/api/config")
def update_config(new_data: FullConfig, db: Session = Depends(get_db)):
    """Updates the config in the database."""
    try:
        services_validated = Services.model_validate(new_data.services)
        config_validated = Config.model_validate(new_data.config)
        canonical_data = {
            "services": services_validated.model_dump(mode="json"),
            "config": config_validated.model_dump(mode="json"),
        }
        record = db.get(ConfigRecord, 1)
        if not record:
            record = ConfigRecord(id=1)
            db.add(record)
        record.config_data = json.dumps(canonical_data, indent=None)  # type:ignore
        db.commit()
        return {"message": "Configuration updated successfully", "status": "ok"}
    except Exception as e:
        logger.error(f"Config update failed: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid configuration data: {e}")


@app.get("/", response_class=HTMLResponse)
async def serve_editor():
    """Serve the configuration editor page."""
    html_file_path = os.path.join(TEMPLATES_DIR, "editor.html")
    if not os.path.exists(html_file_path):
        raise HTTPException(status_code=500, detail="Editor HTML file not found.")

    return FileResponse(html_file_path)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
