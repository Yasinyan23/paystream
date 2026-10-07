import logging
import sys

import structlog
from structlog.typing import EventDict, Processor, WrappedLogger


def _add_service(service: str) -> Processor:
    def processor(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
        event_dict.setdefault("service", service)
        return event_dict

    return processor


def configure_logging(service: str, *, json_logs: bool = True, level: str = "INFO") -> None:
    """Route stdlib logging (our modules, uvicorn, FastStream) through structlog.

    Existing `logging.getLogger(__name__).info("... %s", x)` calls keep working; contextvars bound with
    `structlog.contextvars.bind_contextvars` (request_id, payment_id, ...) are merged into every line.
    """
    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_service(service),
    ]
    renderer: Processor = structlog.processors.JSONRenderer() if json_logs else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            renderer,
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "faststream"):
        named: logging.Logger = logging.getLogger(name)
        named.handlers.clear()
        named.propagate = True
