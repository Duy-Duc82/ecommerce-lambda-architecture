"""BrowserSession: one browser per worker, a fresh context per page, closed once."""
from __future__ import annotations

import pytest

from ecomdata.infra.browser import BrowserSession


class FakePage:
    def evaluate(self, script):
        return "Mozilla/5.0 HeadlessChrome/140"


class FakeContext:
    def __init__(self, options, log):
        self.options, self.log, self.closed = options, log, False

    def new_page(self):
        self.log.append(("page", self.options))
        return FakePage()

    def close(self):
        self.closed = True
        self.log.append(("context_closed", self.options.get("user_agent")))


class FakeBrowser:
    def __init__(self, log):
        self.log, self.contexts, self.closed = log, [], False

    def new_context(self, **options):
        context = FakeContext(options, self.log)
        self.contexts.append(context)
        return context

    def close(self):
        self.closed = True
        self.log.append("browser_closed")


class FakePlaywright:
    def __init__(self, log):
        self.log = log

    def stop(self):
        self.log.append("playwright_stopped")


def session_with(log, **kwargs):
    launches = []

    def launcher():
        launches.append(1)
        return FakePlaywright(log), FakeBrowser(log)

    return BrowserSession(product_token="Bot/0.1 (+thesis)", launcher=launcher, **kwargs), launches


def test_nothing_starts_until_a_page_is_asked_for():
    session, launches = session_with([])

    assert launches == [] and session.closed is False


def test_the_browser_is_launched_once_for_every_page_a_worker_opens():
    session, launches = session_with([])

    for _ in range(3):
        with session.page():
            pass

    assert launches == [1]


def test_the_user_agent_is_the_browsers_own_with_the_projects_token_appended():
    session, _ = session_with([])

    assert session.user_agent == "Mozilla/5.0 HeadlessChrome/140 Bot/0.1 (+thesis)"


def test_each_page_gets_its_own_context_that_is_closed_after_it():
    log = []
    session, _ = session_with(log)

    with session.page():
        pass
    with session.page():
        pass

    browser = session._browser
    page_contexts = [c for c in browser.contexts if c.options.get("locale")]
    assert len(page_contexts) == 2 and all(c.closed for c in browser.contexts)
    assert page_contexts[0] is not page_contexts[1]
    options = page_contexts[0].options
    assert options["locale"] == "vi-VN" and options["user_agent"].endswith("Bot/0.1 (+thesis)")
    assert options["viewport"] == {"width": 1366, "height": 900}


def test_a_context_is_closed_even_when_the_page_work_fails():
    session, _ = session_with([])

    with pytest.raises(RuntimeError, match="boom"):
        with session.page():
            raise RuntimeError("boom")

    assert all(c.closed for c in session._browser.contexts)


def test_closing_stops_the_browser_and_playwright_once_and_forbids_reuse():
    log = []
    session, _ = session_with(log)
    with session.page():
        pass

    session.close()
    session.close()

    assert log.count("browser_closed") == 1 and log.count("playwright_stopped") == 1
    assert session.closed is True
    with pytest.raises(RuntimeError, match="closed"):
        with session.page():
            pass


def test_closing_a_session_that_never_started_launches_nothing():
    session, launches = session_with([])

    session.close()

    assert launches == [] and session.closed is True


def test_the_browser_says_it_is_automated_and_hides_nothing():
    import inspect

    import ecomdata.infra.browser as module

    source = inspect.getsource(module).lower()

    assert "stealth" not in source.replace("no stealth", "").replace("stealth plugin", "")
    assert "webdriver" not in source and "add_init_script" not in source
