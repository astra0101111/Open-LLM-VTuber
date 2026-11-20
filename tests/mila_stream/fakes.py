"""Fake HTTP for tests: scripted responses, records calls."""

from mila_stream.http import HttpResponse


class FakeHttp:
    def __init__(self, routes):
        # routes: url suffix -> list of HttpResponse (consumed in order; last one repeats)
        self.routes = {k: list(v) for k, v in routes.items()}
        self.calls = []

    def _next(self, url):
        for suffix, resps in self.routes.items():
            if url.endswith(suffix):
                return resps.pop(0) if len(resps) > 1 else resps[0]
        return HttpResponse(404, {"error": {"errors": [{"reason": "notFound"}]}})

    async def get(self, url, params, headers=None):
        self.calls.append(("GET", url, dict(params), headers))
        r = self._next(url)
        if isinstance(r, Exception):
            raise r
        return r

    async def post(self, url, json, headers=None):
        self.calls.append(("POST", url, dict(json), headers))
        return self._next(url)

    async def aclose(self):
        pass


async def no_sleep(_s):
    return None
