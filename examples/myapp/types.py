from pydantic import BaseModel


class Payload(BaseModel):
    message: str
    exception_count: int = 0
    routing_key: str = "one"
    second_message: str = ""
    second_message_routing_key: str = "one"
