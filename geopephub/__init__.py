from geopephub.__version__ import __version__
import logmuse
import coloredlogs

_LOGGER = logmuse.init_logger("geopephub")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)

__all__ = ["__version__"]
