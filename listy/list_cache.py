from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, DAILY, WEEKLY, MONTHLY, YEARLY
from dateutil.relativedelta import *

import time
import random
import urllib
import logging
import memcache
import itertools
import cPickle as pickle

try:
    from collections import Counter
except ImportError:
    from listy.counter import Counter

from listy.utils import dump_hex, dict_merge
from listy.on_demand_pickle import OnDemandPickle

# Memcache freaks out at about 2k worth of key data, so we need to batch get_multis into requests
# smaller than that.  The 2k value isn't completely consistent, min seen is 1.9k, max: 2.5k
MAX_KEY_DATA = 1500

log = logging.getLogger(__name__)

list_caches = []

def aggregator(freq, dt):
    dt = dt.date() if isinstance(dt, datetime) else dt
    if freq == DAILY:
        return dt
    elif freq == MONTHLY:
        return date(dt.year, dt.month, 1)
    elif freq == YEARLY:
        return date(dt.year, 1, 1)
    elif freq == WEEKLY:
        return dt - timedelta(days=dt.weekday())

def to_datetime(day):
    "I hate Python's datetime module!"
    return datetime(day.year, day.month, day.day)

RELATIVEDELTA = {
    DAILY: relativedelta(days=+1),
    MONTHLY: relativedelta(months=+1),
    YEARLY: relativedelta(years=+1),
    WEEKLY: relativedelta(weeks=+1),
}

class NotCachableException(Exception):
    pass


class ListCache(object):

    def __init__(self, backing_store, caches, soft_delete_field=None, deleted_timestamp_field=None, enabled_field=None, timestamp_field=None, disable_cache=False, address='127.0.0.1:11211', filter_out_soft_deletes=True):
        """
        Construct a ListCache object.

        Keyword arguments:
        backing_store -- A ListCache backing store abstracton.  Handles querying and writing data to the actual data store.
        caches -- a list of tuples describing the fields that should be keys into the cache
        soft_delete_field -- the name of the delete field that can be used to delete objects without actually removing them from the database if this feature is supported by the model (default None)
        deleted_timestamp_field -- the name of the field which should be set to a datetime when deleting an object (default None)
        enabled_field -- the name of the field which defines whether an object is enabled or not, this is treated like a delete that cannot be undone under normal circumstances (default None)
        timestamp_field -- the name of the field that hold the timestamp to be used for the counters (default None)
        disable_cache -- turn off caching, can be used for debugging (default False)
        address -- a function that returns the address of the memcache (default 127.0.0.1:11211)
        filter_out_soft_deletes -- treat soft deletes as true deletes, filter them out when returning lists (default True)

        The list of tuples defined by the caches argument is the heart
        of this caching mechanism. Through it we define what lists of
        objects we want cached and updated, and how we will access
        those lists.

        For example, if we had a django model called Like that had the
        fields user_id, key, and key_type and our standard access
        pattern was to get all of the likes for a user and all of the
        likes for a type and key then we might want to define the
        following cache:

        cache = ListCache(Like.objects, [('user_id',), ('key', 'key_type')])

        If we defined this then we would be able to get lists from the
        cache using those groups of keys. For example, we could do this:

        likes = cache.get(user_id=1234)
        likes = cache.get(key=5678, key_type='food')

        But we couldn't do this:

        likes = cache.get(key_type='food')

        """
        # TODO: considering adding reverse argument to ListCache and
        # use append and regular order on django queries if not set
        self.backing_store = backing_store
        
        # Hold onto some common backing store values
        self.name = self.backing_store.name()
        self.pickler = self.backing_store.pickler()
        
        self.soft_delete_field = soft_delete_field
        self.deleted_timestamp_field = deleted_timestamp_field
        self.filter_out_soft_deletes = filter_out_soft_deletes
        self.enabled_field = enabled_field
        self.timestamp_field = timestamp_field
        self.disable_cache = disable_cache
        self.address = ('%s:11211' % address()) if callable(address) else address

        self.mc = memcache.Client([self.address],
                                  pickleProtocol=pickle.HIGHEST_PROTOCOL,
                                  pickler=self.pickler, unpickler=self.pickler)

        self.caches_mc = memcache.Client([self.address],
                                         pickleProtocol=pickle.HIGHEST_PROTOCOL)

        self.generation = 1

        # Keep a running list of cache combinations in memcache, and
        # never remove anything from that list, if we want to remove
        # any of the indexes we will need to just flush the entire
        # cache.
        self.configured_caches = set([tuple(sorted(c)) for c in caches])
        self.caches_key = 'caches:%s' % self.name
        self.caches()
        self.reset_local_stats()
        list_caches.append(self)

    def caches(self, update_if_missing=True):
        "Returns a set of the cache indexes that we keep and need to update"
        if self.disable_cache:
            return []
        installed_cache = self.caches_mc.gets(self.caches_key) or set()

        if installed_cache >= self.configured_caches:
            return installed_cache

        for i in range(0, 10):
            c = installed_cache | self.configured_caches
            if self.caches_mc.cas(self.caches_key, c):
                return c

            # If we got back nothing for installed_cache it could be
            # that someone did a cache flush and our cas_ids might be
            # in an inconsistent state. So, we clear them here.
            if not installed_cache:
                self.caches_mc.cas_ids = {}

            c = self.caches_mc.gets(self.caches_key) or set()
            t = 0.001 * random.randint(1, 5) * i
            log.warn("Failed to update 'caches' cache for %r, trying again (%s) in %.3f seconds", self.caches_key, self.address, t)
            time.sleep(t)
        raise RuntimeError("Failed to update 'caches' cache for %r, giving up (%s)", self.caches_key, self.address)

    def count(self, **kwargs):
        "Just like 'get' but returns the count instead of the list. If a datetime is provided"
        key = self.count_key(kwargs)
        count = self.mc.get(key)
        if count is None:
            if self.soft_delete_field:
                kwargs = dict_merge(kwargs, { self.soft_delete_field: False })
            count = self.backing_store.count(kwargs)
            self.mc.set(key, count)
        return count

    def daily_count(self, **kwargs):
        "Just like 'get' but returns the count instead of the list. If a datetime is provided"
        return self._count(DAILY, kwargs)

    def daily_counts(self, **kwargs):
        "Returns a dictionary of date -> count"
        return self._counts(DAILY, kwargs)

    def weekly_count(self, **kwargs):
        "Just like 'get' but returns the count instead of the list. If a datetime is provided"
        return self._count(WEEKLY, kwargs)

    def weekly_counts(self, **kwargs):
        "Returns a dictionary of date -> count"
        return self._counts(WEEKLY, kwargs)

    def monthly_count(self, **kwargs):
        "Just like 'get' but returns the count instead of the list. If a datetime is provided"
        return self._count(MONTHLY, kwargs)

    def monthly_counts(self, **kwargs):
        "Returns a dictionary of date -> count"
        return self._counts(MONTHLY, kwargs)

    def yearly_count(self, **kwargs):
        "Just like 'get' but returns the count instead of the list. If a datetime is provided"
        return self._count(YEARLY, kwargs)

    def yearly_counts(self, **kwargs):
        "Returns a dictionary of date -> count"
        return self._counts(YEARLY, kwargs)

    def get_one(self, pk):
        "Get the object or None if it doesn't exist"
        objects = list(self.get(pk=pk))
        assert len(objects) <= 1
        return objects[0] if objects else None

    def get_first(self, **kwargs):
        "Get the first in the list or None"
        try:
            return iter(self.get(**kwargs)).next()
        except StopIteration:
            return None

    def get(self, **kwargs):
        """
        Get list based on provided query and filtering out deleted and
        disabled items, updates cache if needed.
        """
        return self.get_multi([kwargs], lambda filter_args: 'results')['results']
    
    def get_multi(self, filters, key_func):
        """
        Does a get_multi operation on memcache to request more than one list at the same time.
        
        filters  -- list of dictionaries that you would pass to get() as kwargs
        key_func -- callable that when given a filter, returns a hashable value to be used
                    as a key for the results.
        
        The result is a dictionary of keys (provided by key_func) mapping to lists of objects
        matching the respective filters.
        """
        start = time.time()
        key_map = self.generate_key_map(filters, key_func)
        
        result_set = {}
        if not self.disable_cache:
            for key_batch in self.batch_keys(key_map.keys()):
                result_set.update(self.mc.get_multi(key_batch))

        missing_keys = set(key_map.keys()) - set(result_set.keys())
        
        for missing_key in missing_keys:
            filter_args, cache, user_key = key_map[missing_key]
            results = self.replacement_function(cache, filter_args.items())()
            log.debug("Key %r not in cache, updating with database's results? %r", missing_key, not self.disable_cache)
            if not self.disable_cache:
                if not self.mc.set(missing_key, results):
                    log.info("Set for memcache_key=%s failed, someone must have set it before me.", missing_key)
            
            result_set[missing_key] = results

        self.hits += (len(key_map) - len(missing_keys))
        self.misses += len(missing_keys)
        self.time += (time.time() - start)

        return self.map_results(key_map, result_set)

    def get_multi_list(self, filters):
        "Just like get_multi except returns a flat list of all the values"
        return list(itertools.chain(*self.get_multi(filters, lambda f: repr(f.items())).values()))
    
    def add(self, **kwargs):
        """
        Sets the value, taking into account enabled and deleted flags,
        and updates the various caches.

        1. If it exists and deleted is set to True, get the existing object and set deleted to False
        2. If it doesn't exist, create an object
        3. For each cached list, attempt to prepend the new object onto the list
        4. If the list doesn't exist get all of the objects from the db and set it on the cache
        """
        o = self._set_db(**kwargs)
        if not o:
            log.debug("Not setting add because it already exists in the database")
            return
        
        kwargs['pk'] = self.backing_store.pk_for_object(o)

        for cache in self.caches():
            filtered = [t for t in kwargs.items() if t[0] in cache]
            key = self.key(cache, filtered)
            self.prepend_or_set_memcache(key, o, self.replacement_function(cache, filtered))
            self._update_counters(o, dict(filtered), 1)
        return o

    def delete(self, **kwargs):
        "Delete an item and update the caches"
        objects = self._delete_from_db(**kwargs)
        if not objects:
            return False

        for o in objects:
            object_args = self.backing_store.kwargs_for_object(o, filter_args=kwargs)
            for cache in self.caches():
                filtered = [t for t in object_args if t[0] in cache]
                key = self.key(cache, filtered)
                self.delete_and_replace_memcache(key, self.replacement_function(cache, filtered))
                self._update_counters(o, dict(filtered), -1)
        return True
    
    def rebuild(self, **filter_args):
        """
        Rebuilds the key specified by the filter_args.  This is potentially expensive as it will
        query the backing store!
        """
        object_args = filter_args.items()
        
        for cache in self.caches():
            filtered = [t for t in object_args if t[0] in cache]
            
            if filtered:
                key = self.key(cache, filtered)
                self.delete_and_replace_memcache(key, self.replacement_function(cache, filtered))
    
    def determine_coherency(self, check_against_db=False, dump=False):
        """
        Goes through all objects in the database and confirms that the
        cache is consistent. This could possibly be a very expensive
        method and will certainly mess up the hit rate statistics.
        """
        for o, c, args, key in self.backing_store.all_cachable_objects(self):
            cached_objects = self.mc.get(key)
            if cached_objects is not None:
                cached_objects = list(cached_objects)
            size = getattr(OnDemandPickle, 'last_size', 0) if cached_objects is not None else 0
            count = len(cached_objects or [])
            exists = bool(cached_objects and size and count)

            data = getattr(OnDemandPickle, 'last_data', '') if exists else None
            hex_dump = dump_hex(data) if data and isinstance(data, str) else None

            coherency = {'check_against_db': check_against_db}
            if check_against_db:
                coherency.update(self.check_for_equivalence(c, args, cached_objects))

            yield (c, args, key, exists, hex_dump, size, count, coherency)

    def flush(self, object_args):
        for cache in self.caches():
            filtered = [t for t in object_args if t[0] in cache]
            key = self.key(cache, filtered)
            log.info("Flushing %r", key)
            self.mc.delete(key)

    def flush_all(self):
        "Flushes the entire cache"
        self.mc.flush_all()

    def reset_local_stats(self):
        "Resets the local hits and misses counts"
        self.hits = 0
        self.misses = 0
        self.time = 0.0

    ################################################################################
    
    def key(self, cache, filtered):
        sorted_and_filtered = sorted(filtered, key=lambda t: t[0])
        # NOTE: This will probably assert if there is some other
        # software out there connecting to the cache with a different
        # idea of what should be cached.
        assert len(sorted_and_filtered) > 0, "sorted and filtered from %r and %r" % (cache, filtered)
        # NOTE: The getattr for 'pk' is there so if we ever have
        # values that are model objects instead of strings, for
        # example if we are setting actual Like objects instead of
        # strings then we use the primary key for that instead of the
        # value itself.
        def get_pk(obj):
            pk = self.backing_store.pk_for_object(obj)
            try:
                s = str(pk)
            except UnicodeEncodeError:
                s = self.backing_store.pk_for_object(obj).encode('utf-8')
            return urllib.quote_plus(s)
        
        fields = [('%s=%s' % (k, get_pk(v))).replace(':', '\:') for k, v in sorted_and_filtered]
        key = '%s:v%d:%s' % (self.name, self.generation, ':'.join(fields))
        log.debug("Using key %r", key)
        return key

    def batch_keys(self, keys):
        """
        Take a set of keys and break them into batches small enough to be sent to memcache.
        """
        batches = []
        batch, size = [], 0

        for k in keys:
            # add one to emulate a separator byte, seems to be a bit more consistent with this
            key_len = len(k) + 1
            assert key_len < MAX_KEY_DATA
            
            if size + key_len > MAX_KEY_DATA:
                batches.append(batch)
                batch, size = [], 0
            
            batch.append(k)
            size += key_len
        
        batches.append(batch)
        
        return batches
    
    def replacement_function(self, cache, filtered):
        """
        Query the backing store for values matching the filter so that we can populate the cache
        with the results as appropriate.
        """
        filter_args = dict(filtered)
        if self.soft_delete_field and self.filter_out_soft_deletes:
            filter_args[self.soft_delete_field] = False
        if self.enabled_field:
            filter_args[self.enabled_field] = True
        
        return lambda: self.backing_store.query(filter_args)

    def _set_db(self, **kwargs):
        "Returns the object if the caches need to be updated, None if everything is cool"
        if self.enabled_field:
            kwargs[self.enabled_field] = True
        
        o, created = self.backing_store.get_or_create(**kwargs)
        if not created:
            if self.soft_delete_field:
                if not getattr(o, self.soft_delete_field):
                    # It was already set properly, so there's no need to change anything
                    return None
                
                self.backing_store.update(o, {self.soft_delete_field: False})
            else:
                # This would occur if we tried adding something twice to the database
                return None
        return o

    def _delete_from_db(self, **kwargs):
        "Returns True if deletes something"
        objects = list(self.backing_store.query(kwargs))
        for o in objects:
            if self.soft_delete_field or self.deleted_timestamp_field:
                update_attrs = {}
                if self.soft_delete_field:
                    update_attrs[self.soft_delete_field] = True
                if self.deleted_timestamp_field:
                    update_attrs[self.deleted_timestamp_field] = datetime.utcnow()

                self.backing_store.update(o, update_attrs)

            else:
                self.backing_store.delete(o)

        return objects

    def count_key(self, filter_args, freq=None, timestamp=None):
        if not timestamp or freq is None:
            counter = 'total'
        elif freq == DAILY:
            counter = 'daily.%d-%d-%d' % (timestamp.year, timestamp.month, timestamp.day)
        elif freq == MONTHLY:
            counter = 'monthly.%d-%d' % (timestamp.year, timestamp.month)
        elif freq == YEARLY:
            counter = 'yearly.%d' % (timestamp.year)
        elif freq == WEEKLY:
            counter = 'weekly.%s.%d' % (timestamp.year, timestamp.isocalendar()[1])
        else:
            raise RuntimeException("We shouldn't ever get here, freq = %s", freq)

        filtered = filter_args.items()
        filtered.append(('_counter', counter))
        return self.key(None, filtered)

    def _counts_from_db(self, freq, timestamps, filter_args):
        counts = dict((aggregator(freq, t), 0) for t in timestamps)
        start = to_datetime(aggregator(freq, timestamps[0]))
        end = to_datetime(aggregator(freq, timestamps[-1]) + RELATIVEDELTA[freq])
        if self.soft_delete_field:
            filter_args = dict_merge(filter_args, { self.soft_delete_field: False })
        timestamps_to_count = self.backing_store.timestamps(filter_args, start, end)
        counts.update(Counter([aggregator(freq, dt) for dt in timestamps_to_count]))
        for timestamp, count in counts.items():
            log.debug("Setting cached count of %d for %s", count, timestamp)
            self.mc.set(self.count_key(filter_args, freq, timestamp), count)
        return counts

    def _count(self, freq, filter_args):
        timestamp = filter_args[self.timestamp_field]
        filter_args[self.timestamp_field] = [timestamp.date()]
        counts = self._counts(freq, filter_args)
        return counts[aggregator(freq, timestamp.date())]

    def _counts(self, freq, filter_args):
        """
        The first time it gets a cache miss it goes and asked the db
        for all of the data and then fills in the holes.
        """
        counts = {}
        timestamps = list(filter_args.pop(self.timestamp_field))
        for timestamp in timestamps:
            timestamp = timestamp.date() if isinstance(timestamp, datetime) else timestamp
            count = self.mc.get(self.count_key(filter_args, freq, timestamp))
            if count is None:
                return self._counts_from_db(freq, timestamps, filter_args)
            log.debug("Found count of %d on %s", count, timestamp)
            counts[timestamp] = count
        return counts

    def _update_counters(self, o, filter_args, n):
        filter_args.pop('pk', None)
        timestamp = getattr(o, self.timestamp_field or '', None)
        for freq in [None, DAILY, WEEKLY, MONTHLY, YEARLY] if timestamp else [None]:
            key = self.count_key(filter_args, freq, timestamp)
            if self.mc.get(key) is not None:
                if n > 0:
                    log.debug("Incrementing %s counter", freq or 'total')
                    self.mc.incr(key, n)
                else:
                    log.debug("Decrementing %s counter", freq or 'total')
                    self.mc.decr(key, -n)
            else:
                # Don't do anything here because we shouldn't set
                # counters that aren't being used, otherwise there
                # would be a lot of expensive requests.
                pass

    def prepend_or_set_memcache(self, key, value, all_f):
        """
        Attempts to prepend a value to the cache list, if it doesn't exist
        caches the result of the all_f function.

        Each of the operations are atomic, so we first attempt to prepend,
        if that doesn't work we get all of the objects and set it only if
        it hasn't already been set, if that fails we try the prepend again
        because most likely someone else set the value before we could get
        to it.
        """
        if not self.mc.prepend(key, value):
            log.debug("Couldn't prepend to cache because the object doesn't already exist, setting full object for %r", key)
            if not self.mc.set(key, all_f()):
                log.debug("Check and set failed, someone must have set it before me, falling back to prepend")
                if not self.mc.prepend(key, value):
                    log.error("Prepend failed after trying preprend and set, what is happening!")

    def delete_and_replace_memcache(self, key, all_f):
        if not self.mc.set(key, all_f()):
            log.debug("Set failed, someone must have set it before me, falling back to delete")
            if not self.mc.delete(key):
                log.error("Delete failed, what is happening!")

    def check_for_equivalence(self, c, args, cached_objects):
        """
        Checks for standard object to object equivalence and then
        converts everything to tuples and tests that too. Returns a
        dictionary containing information about the equivalence.
        """
        f = getattr(self.pickler, 'django_object_to_tuple',
                    lambda o: tuple(o.__dict__.items()))

        master_objects = self.replacement_function(c, args)()
        cached_objects = cached_objects or []

        master_tuples = set([tuple(t.items()) if isinstance(t, dict) else t for t in [f(o) for o in master_objects]])
        cached_tuples = set([tuple(t.items()) if isinstance(t, dict) else t for t in [f(o) for o in cached_objects]])

        # NOTE: Why doesn't this use self.pickler?
        #
        #   Something wasn't working properly. I think it had
        #   something to do with not always having a pickler with
        #   those functions, since the standard one doesn't. This is
        #   kinda hacky anyways. My thinking was that I didn't want
        #   this function to blow up if we turned off the mini models.
        p = self.backing_store.pickler()
        t = p.tuple_to_dict

        if cached_tuples != master_tuples:
            return {
                'exclusive_to_cache': [t(o) for o in cached_tuples - master_tuples],
                'exclusive_to_master': [t(o) for o in master_tuples - cached_tuples],
                'cached_objects': [t(o) for o in cached_tuples],
                'master_objects': [t(o) for o in master_tuples],
            }
        return { 'equivalent': True }

    def generate_key_map(self, filters, key_func):
        """
        Make sure our filters fit the cache and return the key map if
        they do. The key map looks like:

           memcache_key -> (filter_args, cache, user_key)
 
       """
        key_map = {}
        caches = self.caches()
        for filter_args in filters:
            cache = tuple(sorted(filter_args.keys()))
            if cache not in caches and not self.disable_cache:
                raise NotCachableException("Keyword args provided (%r) don't match usable caches (%r) for %s" % (cache, caches, self.name))

            key_map[self.key(cache, filter_args.items())] = (filter_args, cache, key_func(filter_args))
        return key_map

    def map_results(self, key_map, result_set):
        """
        This is sort of the converse of generate_key_map. it converts
        the results into a dictionary that can be returned the to
        user.
        """
        results = {}
        for memcache_key, result_list in result_set.items():
            user_key = key_map[memcache_key][2]
            results[user_key] = result_list
        return results


