from scrapy.downloadermiddlewares.retry import RetryMiddleware
from twisted.internet import reactor, defer


class PerRequestDelayMiddleware:
    """
    Middleware that applies per-request delay if request.meta['download_delay'] is set.
    Uses Twisted's callLater to avoid blocking.
    """
    def process_request(self, request, spider):
        delay = request.meta.get("download_delay")
        if delay and delay > 0:
            spider.logger.debug(f"⏳ Delaying request {request.url} by {delay} seconds")

            d = defer.Deferred()
            reactor.callLater(delay, d.callback, None)
            return d
        return None


class CustomRetryMiddleware(RetryMiddleware):
    """
    RetryMiddleware with exponential backoff, integrated with PerRequestDelayMiddleware.
    Instead of blocking with time.sleep, it sets request.meta['download_delay'].
    """
    def __init__(self, settings):
        super().__init__(settings)
        self.base_delay = settings.getint("DOWNLOAD_DELAY", 1)

    def _retry(self, request, reason, spider):
        retries = request.meta.get("retry_times", 0) + 1
        delay = self.base_delay * (2 ** (retries - 1))  # exponential backoff

        spider.logger.warning(
            "🔄 Retrying %(url)s (retry %(count)d), reason: %(reason)s, applying %(delay)d seconds delay",
            {"url": request.url, "count": retries, "reason": reason, "delay": delay},
        )

        # Instead of sleeping, set per-request delay
        new_request = request.copy()
        new_request.meta["download_delay"] = delay
        return super()._retry(new_request, reason, spider)
