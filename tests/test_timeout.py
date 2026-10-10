"""Timeout restoration without waiting for a timer to expire."""

import signal

import pytest

from geopephub.utils import timeout


@pytest.mark.skipif(not hasattr(signal, 'SIGALRM'), reason='requires POSIX timers')
@pytest.mark.parametrize('raises', [False, True])
def test_restores_existing_alarm(raises):
    old_handler = signal.getsignal(signal.SIGALRM)
    old_timer = signal.getitimer(signal.ITIMER_REAL)
    try:
        signal.alarm(60)

        @timeout(120)
        def operation():
            if raises:
                raise RuntimeError('operation failed')
            return 'result'

        if raises:
            with pytest.raises(RuntimeError, match='operation failed'):
                operation()
        else:
            assert operation() == 'result'

        remaining, interval = signal.getitimer(signal.ITIMER_REAL)
        assert 0 < remaining <= 60
        assert interval == 0
        assert signal.getsignal(signal.SIGALRM) == old_handler
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
        signal.setitimer(signal.ITIMER_REAL, *old_timer)


@pytest.mark.skipif(not hasattr(signal, 'SIGALRM'), reason='requires POSIX timers')
def test_elapsed_alarm_is_not_rearmed(monkeypatch):
    old_handler = signal.getsignal(signal.SIGALRM)
    old_timer = signal.getitimer(signal.ITIMER_REAL)
    try:
        signal.alarm(60)
        times = iter([100.0, 161.0])
        monkeypatch.setattr('geopephub.utils.time.time', lambda: next(times))

        @timeout(120)
        def operation():
            return 'result'

        assert operation() == 'result'
        assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        assert signal.getsignal(signal.SIGALRM) == old_handler
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
        signal.setitimer(signal.ITIMER_REAL, *old_timer)


@pytest.mark.skipif(not hasattr(signal, 'SIGALRM'), reason='requires POSIX timers')
def test_nested_timeouts_restore_outer_alarm():
    old_handler = signal.getsignal(signal.SIGALRM)
    old_timer = signal.getitimer(signal.ITIMER_REAL)
    try:
        signal.setitimer(signal.ITIMER_REAL, 0)

        @timeout(120)
        def inner():
            return 'result'

        @timeout(60)
        def outer():
            result = inner()
            remaining, _ = signal.getitimer(signal.ITIMER_REAL)
            assert 0 < remaining <= 60
            return result

        assert outer() == 'result'
        assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        assert signal.getsignal(signal.SIGALRM) == old_handler
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
        signal.setitimer(signal.ITIMER_REAL, *old_timer)
