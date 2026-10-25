"""Validated API and retrieval contracts."""

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class AskRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    question: str = Field(min_length=1, max_length=4000)
    top_k: int = Field(default=3, ge=1, le=20, strict=True)


class Source(BaseModel):
    document_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    title: str = Field(min_length=1)
    event_time: AwareDatetime
    score: float = Field(default=0, allow_inf_nan=False)
    source_url: str | None = None
    ingested_at: AwareDatetime | None = None
    indexed_at: AwareDatetime | None = None


class Document(Source):
    body: str = Field(min_length=1)

    def public_source(self) -> Source:
        return Source.model_validate(self.model_dump(exclude={"body"}))


class AskResponse(BaseModel):
    answer: str
    sources: list[Source]
    retrieval_ms: float = Field(ge=0)
    llm_provider: str
    llm_ms: float = Field(ge=0)
