from collections import defaultdict
from pprint import pformat
import logging

log = logging.getLogger('django_utils.middleware.request_context_cache')

object_registry = defaultdict(dict)

class RequestContextCacheMiddleware(object):
    """
    This middleware exists so you have a place to hang objects during
    a request. It's sort of like an in-memory cache that only exists
    for the duration of the request.
    """
    def process_request(self, request):
        global object_registry
        object_registry.clear()

    def process_response(self, request, response):
        global object_registry
        log.debug("Finished request with: %s", dict((k, len(v)) for k, v in object_registry.iteritems()))
        object_registry.clear()
        return response

