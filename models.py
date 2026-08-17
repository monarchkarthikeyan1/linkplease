from pydantic import BaseModel, Field
from typing import Optional, Any, Dict

class RuleCreate(BaseModel):
    keyword: str
    dm_message: str

class RuleResponse(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str

class UserFrom(BaseModel):
    user_id: str
    username: Optional[str] = None

class CommentData(BaseModel):
    comment_id: str
    post_id: Optional[str] = None
    text: Optional[str] = None
    created_at: Optional[str] = None
    from_: Optional[UserFrom] = Field(default=None, alias="from")

class WebhookPayload(BaseModel):
    event_id: str
    event_type: str  # "comment.created" or "comment.deleted"
    sent_at: Optional[str] = None
    data: CommentData

class StatsResponse(BaseModel):
    sent: int
    failed: int
    queued: int
    duplicates_blocked: int
