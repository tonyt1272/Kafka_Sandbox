from pydantic import BaseModel

class CreatePeopleCommand(BaseModel):
    count: int

class CreateDataCommand(BaseModel):
    count: int
    cols: int