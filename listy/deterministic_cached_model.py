import re
import time
import logging
import traceback
import collections
import cPickle as pickle
from datetime import datetime

from django.db import models
from django.db.models.query import QuerySet
from django.db.models.query_utils import deferred_class_factory
from django.conf import settings

from listy.list_cache import ListCache, NotCachableException
from listy.on_demand_pickle import OnDemandPickle
from listy.utils import dict_merge, filter_into_two, memoized

deferred_class_factory = memoized(deferred_class_factory)

if 'listy.middleware.RequestContextCacheMiddleware' in settings.MIDDLEWARE_CLASSES:
    from listy.middleware import object_registry
else:
    object_registry = collections.defaultdict(lambda: None)

USE_TUPLES_AS_MINI_OBJECTS = False

log = logging.getLogger(__name__)

class no_cache_flush:
    """
    This exists so we can manually update the cache during updates
    within the context of the caching mechanism and still cause
    auto-flushes to occur when people are changing the cache outside
    of the context of the caching mechanism (e.g. the admin panel).
    The reason why we would want to update the cache ourselves when
    possible is because we can do stuff more performant, such as
    prepending to the list rather than just flushing and waiting for
    the next access to update the cache.
    """
    def __init__(self, backing_store):
        # Sometimes we are called with some object that doesn't have a
        # cache, that's ok, we can use ourself to set the attribute
        # and not cause any harm.
        self.cache = getattr(backing_store.manager.model, 'cache', self)
    def __enter__(self):
        self.cache.no_cache_flush = True
    def __exit__(self, type, value, traceback):
        self.cache.no_cache_flush = False


class CachingManager(models.Manager):
    """
    This class adds caching to a model. Cached accesses will need to go
    through the ListCache which is an attribute hanging off of the
    model named 'cache'. ALL updates MUST go through that object. If
    you try to update the cache through me you will get an exception.
    You are allowed to do read queries through me but I'm going to log
    a warning when you do naughty things like that.

    The initialization arguments are exactly those of ListCache.

    Inlined Foreign Keys
    --------------------

    If you provide the parameter inlined_foreign_key_fields to the
    CachingManager it will cache the configured foreign key with the
    data from the configured fields and it will construct deferred
    fields for everything else after you get an object from the cache.
    This means that if you regularly access only a subset of the
    fields from a foreign key you don't need to cache the entire
    object.
    """
    use_for_related_fields = True

    def __init__(self, *args, **kwargs):
        super(CachingManager, self).__init__()
        self.args = args
        self.kwargs = kwargs

    def contribute_to_class(self, model, name):
        models.Manager.contribute_to_class(self, model, name)
        model._rw_objects = self.__class__.__bases__[0]()
        model._rw_objects.contribute_to_class(model, name)
        backing_store_class = self.kwargs.pop('django_backing_store_class', DjangoBackingStore)
        inlined_foreign_key_fields = self.kwargs.pop('inlined_foreign_key_fields', {})
        backing_store = backing_store_class(model._rw_objects, inlined_foreign_key_fields=inlined_foreign_key_fields)
        model.cache = ListCache(backing_store, *self.args, **self.kwargs)

        def shady_save(self, *args, **kwargs):
            # This is shady. Very shady. We are swapping out the
            # _base_manager of the model class before we save becuase we
            # don't want it to use the caching manager which it will use
            # by default when going through the save crap. The stuff we're
            # trying to get around occurs in django.db.models.base:save_base().
            self.__class__._base_manager = self.__class__._rw_objects
            try:
                r = super(model, self).save(*args, **kwargs)
                if not getattr(model.cache, 'no_cache_flush', False):
                    model.cache.flush(model.cache.backing_store.kwargs_for_object(self, {}))
                return r
            finally:
                self.__class__._base_manager = self.__class__.objects

        model.save = shady_save

    def get_query_set(self):
        return CachingQuerySet(self.model)


class CachingQuerySet(QuerySet):
    """
    QuerySet that uses cache only for primary key lookups. Falls back
    to normal db lookups for everything else.

    TODO: throw an exception if you are trying to update the db
    through this.

    Used the following blog posts as inspiration:
       http://www.eflorenzano.com/blog/post/drop-dead-simple-django-caching/
       http://lazypython.blogspot.com/2008_11_01_archive.html
    """

    def filter(self, *args, **kwargs):
        pk = None
        for val in ('pk', 'pk__exact', 'id', 'id__exact'):
            if val in kwargs:
                pk = kwargs[val]
                break
        if pk is not None:
            cache = getattr(self.model, 'cache', None)
            if cache:
                try:
                    result = None
                    registry = object_registry[self.model.__name__]
                    if registry is not None:
                        if pk in registry:
                            log.debug('(1) Got result from object registry for %s and %s', self.model.__name__, pk)
                            result = registry[pk]
                    if not result:
                        result = cache.get_one(pk=pk)
                    if result:
                        if registry:
                            log.debug('(2) Put result in object registry for %s and %s', self.model.__name__, pk)
                            registry[pk] = result
                        # _result_cache is a django internal, we are setting
                        # it so that later calls to this query set will pull
                        # from that.
                        self._result_cache = [result]
                        return self
                except NotCachableException:
                    log.warn("")
        return super(CachingQuerySet, self).filter(*args, **kwargs)

    def get(self, *args, **kwargs):
        clone = self.filter(*args, **kwargs)
        # NOTE: It's possible that what I'm doing is really really
        # bad, I just don't know because I don't understand the code
        # well enough. In Django 1.1 the get function did what this
        # get function does, but in Django 1.2 it added this two extra
        # lines of code:
        # 
        #   if self.query.can_filter():
        #       clone = clone.order_by()
        #
        # Somehow this code seems to cause Django to return everything
        # in the table instead of the query that we pulled out of the
        # cache. I really have no idea why this would happen. But,
        # because this seems like a harmless change I'm doing it.
        num = len(clone)
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise self.model.DoesNotExist("%s matching query does not exist."
                    % self.model._meta.object_name)
        raise self.model.MultipleObjectsReturned("get() returned more than one %s -- it returned %s! Lookup parameters were %s"
                % (self.model._meta.object_name, num, kwargs))



class DjangoBackingStore(object):
    """
    A ListCache backing store abstraction for Django models.
    
    This class add deterministic caching to django models (i.e.,
    caching that keeps the caches as in-sync with the database as
    humanly possible). Instead of just supporting primary key lookups,
    it also supports lists of objects. I've tried to optimize for
    correctness.
    """
    def __init__(self, manager, inlined_foreign_key_fields=None):
        self.manager = manager
        self.inlined_foreign_key_fields = inlined_foreign_key_fields or {}
    
    def name(self):
        """
        A unique name to use for identifying the cache's "domain".
        
        This is used as part of the memcache key, so should be a simple string.
        """
        return self.manager.model.__name__
    
    def pickler(self):
        """
        Returns a pickler to use when serializing/deserializing data into the cache.
        
        This should default to OnDemandPickle if you do not have a custom pickler.
        """
        return generate_mini_model_pickler(self.manager.model, self.inlined_foreign_key_fields)
    
    def query(self, filter_args):
        """
        Perform a query on the backing store, returning matching entires.  This is used to fill in 
        missing/stale cache keys.
        
        Note, this is a bit hacky: You can possibly get a filter_arg of 'pk', which you should treat
        as a query for whatever value you returned via pk_for_object().
        
        The filter is a simple dict of key/value pairs to be ANDed together.  If filter_args is an
        empty dict, you should return all objects in the collection.
        
        The result should be sorted by decreasing recency.
        """
        query_set = self.manager.filter(**filter_args).order_by('-id')
        if self.inlined_foreign_key_fields:
            fields = []
            for f, keys in self.inlined_foreign_key_fields.items():
                fields.extend(('__'.join((f, k)) for k in keys))
            query_set = query_set.select_related(*fields)
        return query_set

    def count(self, filter_args):
        """
        Just like 'query' except returns the count instead of the objects.
        """
        return self.manager.filter(**filter_args).count()

    def timestamps(self, filter_args, start, end):
        field = self.manager.model.cache.timestamp_field
        date_filter = { field + '__gte': start, field + '__lt': end }
        return self.manager.filter(**dict_merge(filter_args, date_filter)).values_list(field, flat=True)
    
    def get_or_create(self, **kwargs):
        """
        Gets or creates the object.  Conforms the Django's logic for get_or_create().
        
        Remember, the key 'defaults' is special.  Any keys *other* that defaults should be used as
        a filter (as well as base value).  anything in the defaults key (which is a dict its self)
        get appended as additional values for the object.
        
        See: http://docs.djangoproject.com/en/1.1/ref/models/querysets/#get-or-create-kwargs

        TODO: This would be much cleaner if it were get_or_create(self, filter_keys, create_defaults),
        or something similar. Having magic keywords sucks.

        """
        with no_cache_flush(self):
            return self.manager.get_or_create(**kwargs)
    
    def update(self, obj, fields):
        """
        Takes an object returned by query() or get_or_create() with a dict of fields to update and
        their new values.
        """
        for key, value in fields.items():
            setattr(obj, key, value)
        with no_cache_flush(self):
            obj.save()
    
    def delete(self, obj):
        """
        Removes the given object from the backing store.
        """
        with no_cache_flush(self):
            obj.delete()
    
    def kwargs_for_object(self, obj, filter_args):
        """
        Given a cacheable object that we return, return the list of key/value tuples that can be
        used to query and identify it.
        
        It's a little more complex than you would think because for foreign keys we use the name of
        the regular attribute instead of the one with the implicitely added suffix '_id' that is
        returned in the list of fields or __dict__.
        
        We also pass in the filter_args used to find this object, for contextual references.
        """
        object_args = [('pk', obj.pk)]
        
        for f in obj._meta.fields:
            k = f.get_attname()
            v = getattr(obj, k)
            if isinstance(f, models.ForeignKey):
                k = re.sub(r'_id$', '', k)
            
            object_args.append((k, v))
        
        return object_args
    
    def pk_for_object(self, obj):
        """
        Returns the primary key for an object.  This can be any value that uniquely identifies it
        within the collection.
        
        You may also recieve primary keys rather than a full object, and are expected to simply
        return them.
        """
        return getattr(obj, 'pk', obj)
    
    def all_cachable_objects(self, list_cache):
        "Returns a generator of objects and the various things needed to get the objects out of the cache."
        keys = set()
        for o in self.query({}):
            for c in list_cache.caches():
                args = [(k, getattr(o, k)) for k in c]
                key = list_cache.key(c, (args))
                if key in keys:
                    continue
                keys.add(key)
                
                yield (o, c, args, key)


def generate_mini_model_pickler(model, inlined_foreign_key_fields):
    def django_object_to_tuple(o):
        "Takes in a model object and returns a tuple that can be used for serialization"
        if USE_TUPLES_AS_MINI_OBJECTS:
            result = []
            tuples = [(f.get_attname(), o.serializable_value(f.get_attname())) for f in sorted(o._meta.fields)]
            for k, v in tuples:
                if isinstance(v, datetime):
                    v = time.mktime(v.timetuple())
                result.append(v)
            for foreign_key, attributes in inlined_foreign_key_fields.iteritems():
                foreign_object = getattr(o, foreign_key)
                attributes = [(a, getattr(foreign_object, a)) for a in attributes]
                attributes.append(('id', foreign_object.id))
                result.append(('GeB', foreign_key, attributes))
            return tuple(result)
        else:
            result = dict(((f.get_attname(), o.serializable_value(f.get_attname())) for f in o._meta.fields))
            for foreign_key, attributes in inlined_foreign_key_fields.iteritems():
                foreign_object = getattr(o, foreign_key)
                attributes = [(a, getattr(foreign_object, a)) for a in attributes]
                attributes.append(('id', foreign_object.id))
                result[foreign_key] = ('GeB', foreign_key, attributes)
            return result

    def tuple_to_dict(t):
        "Takes in a model and a tuple and returns a dictionary"
        fields = sorted(model._meta.fields)
        assert len(fields) == len(t)
        kwargs = {}
        for k, v in zip(fields, t):
            if isinstance(k, models.DateField):
                v = datetime.fromtimestamp(v) if v else None
            kwargs[k.get_attname()] = v
        return kwargs

    def construct(model, kwargs, foreigners):
        "Reconstructs object and inlined foreigners with provided data and defered attributes"
        o = model(**kwargs)
        o._state.adding = False
        for f in foreigners:
            _, foreign_key, attributes = f
            attributes = dict(attributes)
            foreign_model = model._meta.get_field(foreign_key).related.parent_model

            registry = object_registry[foreign_model.__name__]
            foreign_object = None
            if registry is not None:
                pk = attributes[foreign_model._meta.pk.name]
                if pk in registry:
                    foreign_object = registry[pk]

            if foreign_object is None:
                deferred_keys = (set([field.name for field in foreign_model._meta.fields if not isinstance(field, models.ForeignKey)]) - set(attributes.keys()))
                foreign_class = deferred_class_factory(foreign_model, deferred_keys)
                foreign_object = foreign_class(**attributes)
                foreign_object._state.adding = False
                # NOTE: This is causing a failure and I'm not sure why so
                # for now we don't get out foreign objects on the registry.
                # It has something to do with someone else trying to get the
                # full object and getting this instead. So, for now we turn
                # it off.
                #
                # if registry is not None and pk not in registry:
                #    registry[pk] = foreign_object

            setattr(o, foreign_key, foreign_object)
        return o

    def tuple_to_django_object(t):
        "Takes in a model and a tuple returns a model object"
        if isinstance(t, tuple):
            foreigners, t = filter_into_two(t, lambda e: isinstance(e, tuple) and e[0] == 'GeB')
            kwargs = tuple_to_dict(t)
        elif isinstance(t, dict):
            foreigners = []
            for k, e in t.items():
                if isinstance(e, tuple) and e[0] == 'GeB':
                    foreigners.append(t.pop(k))
            kwargs = t
        else:
            return t

        registry = object_registry[model.__name__]
        if registry is not None:
            pk = kwargs[model._meta.pk.name]
            if pk in registry:
                log.debug('(3) Got result from object registry for %s and %s', model.__name__, pk)
                return registry[pk]
            o = construct(model, kwargs, foreigners)
            log.debug('(4) Put result in object registry for %s and %s', model.__name__, pk)
            registry[pk] = o
            return o

        return construct(model, kwargs, foreigners)

    class PackedOnDemandPickle(OnDemandPickle):
        """
        Behaves exactly like the OnDemandPickle but packs everything down
        into tuples so they take less space.
        """
        def __init__(self, f, protocol=pickle.HIGHEST_PROTOCOL):
            OnDemandPickle.__init__(self, f, protocol, django_object_to_tuple, tuple_to_django_object)

    PackedOnDemandPickle.django_object_to_tuple = staticmethod(django_object_to_tuple)
    PackedOnDemandPickle.tuple_to_dict = staticmethod(tuple_to_dict)
    PackedOnDemandPickle.tuple_to_django_object = staticmethod(tuple_to_django_object)

    return PackedOnDemandPickle
