from pydantic import BaseModel

class Message(BaseModel):
    sender: str
    receiver: str
    content: str

class EditMessage(BaseModel):
    id: str
    content: str

class RequestVote(BaseModel):
    term: int
    candidate_id: str

class AppendEntries(BaseModel):
    term: int
    leader_id: str
