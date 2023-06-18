from pydantic import BaseModel

class Person(BaseModel):
    id: str
    name: str
    title: str

class Data(BaseModel):
    sample: str
    cols: str
    data_values: str