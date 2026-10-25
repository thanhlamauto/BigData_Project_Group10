"""FastAPI application factory. Run from the TV5 directory."""

from contextlib import asynccontextmanager
from time import perf_counter

import httpx
from fastapi import FastAPI, HTTPException, Request

from .config import Settings
from .llm import HTTPLLM, LLM, LLMError, MockLLM
from .models import AskRequest, AskResponse
from .prompt import INSUFFICIENT_CONTEXT, build_prompt
from .retrieval import (
    FakeRetriever,
    OpenSearchRetriever,
    RetrievalError,
    Retriever,
    load_embedder,
    newest_unique,
)


def create_app(
    settings: Settings | None = None, retriever: Retriever | None = None, llm: LLM | None = None
) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        with (
            httpx.Client(timeout=settings.retrieval_timeout_seconds) as search_client,
            httpx.Client(timeout=settings.llm_timeout_seconds) as llm_client,
        ):
            if retriever is not None:
                app.state.retriever = retriever
            elif settings.retriever_backend == "fake":
                app.state.retriever = FakeRetriever.from_jsonl(settings.fixtures_path)
            else:
                auth = (
                    (settings.opensearch_username, settings.opensearch_password.get_secret_value())
                    if settings.opensearch_username
                    else None
                )
                app.state.retriever = OpenSearchRetriever(
                    search_client,
                    settings.opensearch_url,
                    settings.opensearch_index,
                    load_embedder(settings.embedder_factory),
                    settings.embedding_dim,
                    settings.opensearch_vector_field,
                    auth,
                )
            app.state.llm = llm or (
                MockLLM()
                if settings.llm_provider == "mock"
                else HTTPLLM(
                    llm_client,
                    settings.llm_provider,
                    settings.llm_base_url,
                    settings.llm_model,
                    settings.llm_api_key.get_secret_value(),
                )
            )
            yield

    app = FastAPI(title="StreamRAG API", version="1.0.0", lifespan=lifespan)

    @app.get("/health")
    def health():
        # Process liveness, not a full upstream readiness probe.
        return {
            "status": "ok",
            "retriever_backend": settings.retriever_backend,
            "llm_provider": settings.llm_provider,
        }

    @app.post("/ask", response_model=AskResponse)
    def ask(payload: AskRequest, request: Request):
        started = perf_counter()
        try:
            documents = newest_unique(
                request.app.state.retriever.retrieve(payload.question, payload.top_k)
            )[: payload.top_k]
        except RetrievalError as exc:
            raise HTTPException(status_code=503, detail="Retrieval backend unavailable") from exc
        retrieval_ms = (perf_counter() - started) * 1000
        llm_started = perf_counter()
        answer = INSUFFICIENT_CONTEXT
        if documents:
            try:
                answer = request.app.state.llm.generate(build_prompt(payload.question, documents))
            except LLMError as exc:
                raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
        llm_ms = (perf_counter() - llm_started) * 1000 if documents else 0
        return AskResponse(
            answer=answer,
            sources=[document.public_source() for document in documents],
            retrieval_ms=round(retrieval_ms, 3),
            llm_provider=settings.llm_provider,
            llm_ms=round(llm_ms, 3),
        )

    return app


app = create_app()
