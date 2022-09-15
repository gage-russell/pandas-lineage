import logging

logger = logging.getLogger()


EMISSION_ERROR_STR = "WARNING: Request Error: failed to emit lineage event: {message}"


class LineageEmissionError(Exception):
    pass


def silenceable_failure(silenced: bool, message: str, error_type: type):
    if silenced:
        logger.warning(message)
    else:
        raise error_type(message)
