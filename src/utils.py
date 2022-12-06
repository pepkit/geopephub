import time
import signal
import geofetch
from typing import Dict
import peppy


class FunctionTimeoutError(Exception):
    """
    Time out error when function is running too long
    """

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.
        :param str reason: some context or perhaps just a value that
            could not be interpreted as an accession
        """
        super(FunctionTimeoutError, self).__init__(reason)


# TODO: consider to change it to: https://github.com/pnpnpn/timeout-decorator
def timeout(seconds_before_timeout=60):
    def decorate(f):
        def handler(signum, frame):
            raise FunctionTimeoutError("Geofetch running time is too long. TimeOut.")

        def new_f(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handler)
            old_time_left = signal.alarm(seconds_before_timeout)
            if (
                0 < old_time_left < seconds_before_timeout
            ):  # never lengthen existing timer
                signal.alarm(old_time_left)
            start_time = time.time()
            try:
                result = f(*args, **kwargs)
            finally:
                if old_time_left > 0:  # deduct f's run time from the saved timer
                    old_time_left -= time.time() - start_time
                signal.signal(signal.SIGALRM, old)
                signal.alarm(old_time_left)
            return result

        return new_f

    return decorate


@timeout(120)
def run_geofetch(
    gse: str, geofetcher_obj: geofetch.Geofetcher = None
) -> Dict[str, peppy.Project]:
    """
    geofetch wrapped in function
    :param gse: Projects GSE
    :param geofetcher_obj: object of Geofetcher class
    :return: dict of peppys
    """
    if not geofetcher_obj:
        geofetcher_obj = geofetch.Geofetcher()
    project_dict = geofetcher_obj.get_projects(gse)
    return project_dict
