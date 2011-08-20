from pprint import pprint
from datetime import datetime, timedelta, date
from dateutil.rrule import rrule, DAILY, WEEKLY, MONTHLY, YEARLY
import types
import logging
import memcache

from django.test import TestCase
from django.db.models.query import QuerySet
from django.db import connection, transaction
from django.conf import settings 

from listy.list_cache import ListCache, NotCachableException

from test_django_app.caching.models import *
from listy.deterministic_cached_model import DjangoBackingStore
from listy.middleware import RequestContextCacheMiddleware, object_registry

log = logging.getLogger('listy')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
log.addHandler(ch)

def make_cache(model, *args, **kwargs):
    """
    Quick helper to build a ListCache w/ a DjangoBackingStore
    """
    return ListCache(DjangoBackingStore(model.objects), *args, **kwargs)

class CachingTest(TestCase):
    def setUp(self):
        self.blog = Blog.objects.create()
        self.entry = self.blog.entries.create(title='Funny Blog Post')
        memcache.Client(['127.0.0.1:11211']).flush_all()
        RequestContextCacheMiddleware().process_request(None)

    def tearDown(self):
        RequestContextCacheMiddleware().process_response(None, None)

    def assert_not_cached(self, o):
        self.assertTrue(isinstance(o, QuerySet))

    def assert_cached(self, o):
        self.assertFalse(isinstance(o, QuerySet))
        self.assertTrue(isinstance(o, types.GeneratorType))

    def test_add_new(self):
        "Tests that adding through to the database works and that it also got added to the cache"
        self.assertFalse(Tag.cache.mc.get('Tag:v1:color=1:entry=1'))
        Tag.cache.add(entry=self.entry, name='ska', color=1)
        self.assertEqual(1, self.entry.tags.count())
        self.assertTrue(Tag.cache.mc.get('Tag:v1:color=1:entry=1'))

    def test_add_existing_no_op(self):
        "Just tests that adding the same thing over again is a no op"
        Tag.cache.add(entry=self.entry, name='ska', color=1)
        Tag.cache.add(entry=self.entry, name='ska', color=1)
        self.assertEqual(1, self.entry.tags.count())

        cache1 = make_cache(Tag, [('entry',)])
        cache1.add(entry=self.entry, name='ska', color=1)
        cache1.add(entry=self.entry, name='ska', color=1)
        self.assertEqual(1, len(list(cache1.get(entry=self.entry))))

    def test_add_existing_already_deleted(self):
        "Tests that we deal properly with adding back things that were soft deleted"
        self.entry.tags.create(name='ska', color=1, deleted=True)
        self.assertEqual(1, self.entry.tags.count())
        Tag.cache.add(entry=self.entry, name='ska', color=1)
        self.assertEqual(1, self.entry.tags.count())

    def test_add_with_not_enough_args(self):
        self.assertRaises(Exception, lambda: Tag.cache.add(entry=self.entry))

    def test_get_not_cached(self):
        "Test that the first time we get something it's not cached, but the second time it is"
        self.entry.tags.create(name='ska', color=1)
        EXPECTED = "[<Tag: <Entry: 1>, u'ska', 1, True, False, None>]"

        # Won't be in the cache the first time around
        results = Tag.cache.get(entry=self.entry, color=1)
        self.assert_not_cached(results)
        self.assertEqual(EXPECTED, str(results))

        # Should be cached by now
        results = Tag.cache.get(entry=self.entry, color=1)
        self.assert_cached(results)
        self.assertEqual(EXPECTED, str(list(results)))

    def test_end_to_end(self):
        "Test that our whole add/get thing works"
        entry2 = self.blog.entries.create()
        Tag.cache.add(entry=self.entry, name='ska', color=1)
        Tag.cache.add(entry=entry2, name='ska', color=1)

        # First test that we can get with ('entry', 'color')
        EXPECTED = "[<Tag: <Entry: 1>, u'ska', 1, True, False, None>]"
        results = Tag.cache.get(entry=self.entry, color=1)
        self.assert_cached(results)
        self.assertEqual(EXPECTED, str(list(results)))

        # Next test that we can get with ('name', 'color')
        EXPECTED = "[<Tag: <Entry: 2>, 'ska', 1, True, False, None>, <Tag: <Entry: 1>, u'ska', 1, True, False, None>]"
        results = Tag.cache.get(name='ska', color=1)
        self.assert_cached(results)
        self.assertEqual(EXPECTED, str(list(results)))

    def test_get_doesnt_exist(self):
        "Tests that the case when something doesn't exist still works and that it gets cached"
        EXPECTED = "[]"

        # Won't be in the cache the first time around (empty result)
        results = Tag.cache.get(entry=self.entry, color=1)
        self.assert_not_cached(results)
        self.assertEqual(EXPECTED, str(results))

        # Should be cached by now (empty result)
        results = Tag.cache.get(entry=self.entry, color=1)
        self.assert_cached(results)
        self.assertEqual(EXPECTED, str(list(results)))

    def test_get_with_not_enough_args(self):
        self.assertRaises(NotCachableException, lambda: Tag.cache.get(entry=self.entry))

    def test_primary_key_get(self):
        "Tests that we can get single objects with the primary"
        self.assertEqual(None, Tag.cache.get_one(1))
        Tag.cache.add(entry=self.entry, name='ska', color=1)
        self.assertEqual("<Entry: 1>, 'ska', 1, True, False, None", str(Tag.cache.get_one(1)))

    def test_primary_key_get_change_foreign_key(self):
        "Tests that we don't cache the foreign key object, so if it changes we are still good"
        self.assertEqual(None, Tag.cache.get_one(1))
        Tag.cache.add(entry=self.entry, name='ska', color=1)

        connection.cursor().execute('UPDATE caching_entry SET title = "This has been changed"')
        transaction.commit_unless_managed()


        self.assertEqual('This has been changed', Tag.cache.get_one(1).entry.title)

    def test_get_first(self):
        "Tests that we get back only one object or None"
        self.entry.tags.create(name='ska', color=1)
        self.entry.tags.create(name='meatloaf', color=1)
        cache = make_cache(Tag, [('color',)])
        self.assertEqual(2, len(list(cache.get(color=1))))
        self.assertTrue(isinstance(cache.get_first(color=1), Tag))
        self.assertEqual(None, cache.get_first(color=2))

    def test_get_multi(self):
        "Tests that get multi will return some stuff from the cache and some stuff from the db"
        self.entry.tags.create(name='ska', color=1)
        self.entry.tags.create(name='meatloaf', color=2)
        # Force just one of them into the cache
        Tag.cache.get(entry=self.entry, color=1)
        results = Tag.cache.get_multi([{'entry': self.entry, 'color': 1},
                                        {'entry': self.entry, 'color': 2}],
                                       lambda a: a['color'])
        self.assert_cached(results[1])
        self.assert_not_cached(results[2])

    def test_get_multi_list(self):
        "Tests that get multi list will return a list"
        self.entry.tags.create(name='ska', color=1)
        self.entry.tags.create(name='meatloaf', color=2)
        tags = Tag.cache.get_multi_list([{'entry': self.entry, 'color': 1}, {'entry': self.entry, 'color': 2}])
        self.assertEqual(2, len(tags))

    def test_primary_key_delete(self):
        "Tests that when we delete with a primary key, all of the caches get updated"
        self.assertEqual(None, Tag.cache.get_one(1))
        o = Tag.cache.add(entry=self.entry, name='ska', color=1)
        pk = o.pk
        Tag.cache.delete(pk=pk)
        self.assertEquals(None, Tag.cache.get_one(pk))
        self.assertEquals([], list(Tag.cache.get(entry=self.entry, color=1)))
        self.assertEquals([], list(Tag.cache.get(name='ska', color=1)))

    def test_delete_to_nothing(self):
        "After deleting something the deltes it from the cache too"
        self.entry.tags.create(name='ska', color=1)
        self.assert_not_cached(Tag.cache.get(entry=self.entry, color=1))
        self.assert_cached(Tag.cache.get(entry=self.entry, color=1))

        self.assertTrue(Tag.cache.delete(entry=self.entry, name='ska', color=1))

        self.assertEqual(1, self.entry.tags.count())
        results = Tag.cache.get(entry=self.entry, color=1)
        self.assertEqual([], list(results))

    def test_delete_to_something_smaller(self):
        "After deleting something, we delete it from the cache too even if there is stuff left for those keys"
        self.entry.tags.create(name='ska', color=1)
        self.entry.tags.create(name='meatloaf', color=1)
        self.assert_not_cached(Tag.cache.get(entry=self.entry, color=1))
        self.assert_cached(Tag.cache.get(entry=self.entry, color=1))

        self.assertTrue(Tag.cache.delete(entry=self.entry, name='ska', color=1))
        self.assertEqual(2, self.entry.tags.count())
        results = Tag.cache.get(entry=self.entry, color=1)
        EXPECTED = "[<Tag: <Entry: 1>, u'meatloaf', 1, True, False, None>]"
        self.assertEqual(EXPECTED, str(list(results)))

    def test_caches_changed(self):
        """
        Bad bad stuff can happen if we have two clients who disagree on
        what should be cached. If this happens we could easily get
        into a state where someone is changing the database without
        updating the cache that another guy relies on.
        """
        cache1 = make_cache(Tag, [('entry',), ('name',)])
        cache2 = make_cache(Tag, [('entry',)])
        cache3 = make_cache(Tag, [('entry',), ('color',)])

        # All of the caches should agree when they are caching something that they all store
        cache1.add(entry=self.entry, name='ska', color=1)
        EXPECTED = "[<Tag: <Entry: 1>, u'ska', 1, True, False, None>]"
        self.assert_cached(cache1.get(entry=self.entry))
        self.assert_cached(cache2.get(entry=self.entry))
        self.assert_cached(cache3.get(entry=self.entry))
        self.assertEqual(EXPECTED, str(list(cache1.get(entry=self.entry))))
        self.assertEqual(EXPECTED, str(list(cache2.get(entry=self.entry))))
        self.assertEqual(EXPECTED, str(list(cache3.get(entry=self.entry))))

        # It's starts getting bad when some we start updating caches
        # for stuff that isn't always agreed upon. But as long as
        # there are no existing entries we are still fine.
        self.assert_cached(cache1.get(name='ska'))

        # Since we keep the union of all of the caches that all of the
        # client care about this works
        self.assert_cached(cache3.get(color=1))

        # Now we update from cache1 again and the ('color',) index for
        # color=1 could become stale (but it doesn't because our code
        # works)
        cache1.add(entry=self.entry, name='meatloaf', color=1)
        EXPECTED = "[<Tag: <Entry: 1>, 'meatloaf', 1, True, False, None>, <Tag: <Entry: 1>, u'ska', 1, True, False, None>]"
        self.assert_cached(cache3.get(color=1))
        self.assertEqual(EXPECTED, str(list(cache3.get(color=1))))

    def test_cache_coherency(self):
        for i in range(1, 3):
            Tag.cache.add(entry=self.entry, name='ska', color=i)
        for n in ['meatloaf', 'mashed potatoes', 'turkey', 'salami', 'gravy']:
            Tag.cache.add(entry=self.entry, name=n, color=i)

        for c, args, key, exists, hex_dump, size, count, coherency in Tag.cache.determine_coherency(True, True):
            pass

    def test_foreign_keys(self):
        "Confirms that we properly deal with cached foreign keys"
        Tag.cache.add(entry=self.entry, name='foreign key test', color=1)

        # Change the title so that we know that this didn't come from the cache
        connection.cursor().execute('UPDATE caching_entry SET title = "This title has been CHANGED!!!!"')
        transaction.commit_unless_managed()

        results = Tag.cache.get(name='foreign key test', color=1)
        self.assert_cached(results)
        self.assertEqual("This title has been CHANGED!!!!", list(results)[0].entry.title)

    def test_foreign_keys_with_inlined_fields(self):
        "Tests that we can inline certain fields in the cached objects so that a separate key lookup isn't required"
        cache1 = ListCache(DjangoBackingStore(Tag.objects, inlined_foreign_key_fields={'entry': ['created']}), [('name', 'color')])
        cache1.add(entry=self.entry, name='poop', color=1)
        o = cache1.get_first(name='poop', color=1)
        self.assertTrue(hasattr(o, '_entry_cache'))
        self.assertTrue(isinstance(o.entry.created, datetime))
        self.assertFalse('title' in o.entry.__dict__)
        self.assertEqual('Funny Blog Post', o.entry.title)

    def test_without_soft_delete(self):
        cache1 = make_cache(Tag, [('entry',), ('name',)])
        cache1.add(entry=self.entry, name='ska', color=1)
        cache1.add(entry=self.entry, name='ska', color=1)
        cache1.delete(entry=self.entry, name='ska', color=1)
        cache1.add(entry=self.entry, name='ska', color=1)

    def test_cache_something_with_non_identity_field(self):
        """
        This tests that we can cache objects that don't use all of their fields for their identity
        """
        cache1 = make_cache(Blog, [('name',)])
        cache1.add(name='My Cool Blog', description="This is the place where I put down all of my thoughts and everyone will think I'm cool and stuff. And I know everyone will read this blog because I really popular like that.")
        self.assert_cached(cache1.get(name='My Cool Blog'))

    def test_dont_filter_out_soft_deletes(self):
        cache1 = make_cache(Tag, [('entry', 'color'), ('name', 'color'), ('pk',)], 'deleted', 'deleted_timestamp', 'enabled', filter_out_soft_deletes=False)
        cache1.add(entry=self.entry, name='ska', color=1)
        cache1.add(entry=self.entry, name='meatloaf', color=1)
        cache1.delete(entry=self.entry, name='meatloaf', color=1)
        self.assert_cached(cache1.get(entry=self.entry, color=1))
        results = list(cache1.get(entry=self.entry, color=1))
        self.assertEqual(2, len(results))
        self.assertTrue(results[0].deleted)

    def test_foreign_key_from_cache(self):
        "Tests that when foreign keys are accessed we attempt to get them from the cache"
        settings.DEBUG = True
        Tag.cache.add(entry=self.entry, name='ska', color=1)

        t = Tag.cache.get_one(1)
        connection.queries = []
        t.entry
        self.assertEqual(1, len(connection.queries))

        t = Tag.cache.get_one(1)
        connection.queries = []
        t.entry
        self.assertEqual(0, len(connection.queries))

        t.entry.blog # This guy shouldn't throw or anything

        settings.DEBUG = False
    
    def test_update_item(self):
        "This tests a weird edge case"
        e1 = self.entry
        e2 = self.blog.entries.create(title='Second one')
        e2.title = 'foo bar'
        e2.save()

    def test_basic_counters(self):
        "Tests that add and delete both do the right things with the counters"
        self.assertEqual(1, Entry.cache.count())
        self.assertEqual(1, Entry.cache.daily_count(created=self.entry.created))
        self.assertEqual(1, Entry.cache.weekly_count(created=self.entry.created))
        self.assertEqual(1, Entry.cache.monthly_count(created=self.entry.created))
        self.assertEqual(1, Entry.cache.yearly_count(created=self.entry.created))

        self.assertEqual(0, Tag.cache.count())
        self.assertEqual(0, Tag.cache.count(entry=self.entry, color=1))
        self.assertEqual(0, Tag.cache.count(name='ska', color=1))
        
        Tag.cache.add(entry=self.entry, name='ska', color=1)
        self.assertEqual(1, Tag.cache.count(entry=self.entry, color=1))

    def test_delete_counter_from_cache(self):
        """
        Tests what happens when we add counting to something that didn't have it before,
        increment should first fill in the missing info, not just blindly increment
        """
        Tag.cache.add(entry=self.entry, name='ska', color=1)
        Tag.cache.add(entry=self.entry, name='tacos', color=1)
        self.assertEqual(2, Tag.cache.count(entry=self.entry, color=1))

        self.assertTrue(Tag.cache.mc.delete('Tag:v1:_counter=total:color=1:entry=1'))
        Tag.cache.add(entry=self.entry, name='buritos', color=1)
        self.assertEqual(3, Tag.cache.count(entry=self.entry, color=1))

    def test_timestamp_counters(self):
        "Tests that if a timestamp exists we use it to also cache the counter by various time granularities"
        self.assertEqual(1, Entry.cache.daily_count(created=self.entry.created))
        self.assertEqual(0, Entry.cache.daily_count(created=self.entry.created-timedelta(days=1)))

    def test_daily_counters(self):
        start = date(2011, 1, 1)
        end = date(2011, 1, 9)
        self.create_entry_on_day(date(2011, 1, 1), 3)
        self.create_entry_on_day(date(2011, 1, 4), 1)
        self.create_entry_on_day(date(2011, 1, 8), 2)
        EXPECTED = {
            date(2011, 1, 1): 3,
            date(2011, 1, 2): 0,
            date(2011, 1, 3): 0,
            date(2011, 1, 4): 1,
            date(2011, 1, 5): 0,
            date(2011, 1, 6): 0,
            date(2011, 1, 7): 0,
            date(2011, 1, 8): 2,
            date(2011, 1, 9): 0,
        }

        # Nothing will be cached here
        self.assertEqual(EXPECTED, Entry.cache.daily_counts(created=rrule(DAILY, dtstart=start, until=end)))

        # Everything will be cached here
        self.assertEqual(EXPECTED, Entry.cache.daily_counts(created=rrule(DAILY, dtstart=start, until=end)))

        # Everything except for one count will be cached here
        self.assertTrue(Entry.cache.mc.delete('Entry:v1:_counter=daily.2011-01-04'))
        self.assertEqual(EXPECTED, Entry.cache.daily_counts(created=rrule(DAILY, dtstart=start, until=end)))
        
        # Now try to find the count with extra args
        EXPECTED = {
            date(2011, 1, 1): 1,
            date(2011, 1, 2): 0,
            date(2011, 1, 3): 0,
            date(2011, 1, 4): 0,
            date(2011, 1, 5): 0,
            date(2011, 1, 6): 0,
            date(2011, 1, 7): 0,
            date(2011, 1, 8): 0,
            date(2011, 1, 9): 0,
        }
        self.assertEqual(EXPECTED, Entry.cache.daily_counts(created=rrule(DAILY, dtstart=start, until=end), title='2011-01-01.0'))

    def test_weekly_counters(self):
        start = date(2010, 1, 1)
        end = date(2010, 2, 1)
        self.create_entry_on_day(date(2010, 1, 1), 3)
        self.create_entry_on_day(date(2010, 1, 4), 1)
        self.create_entry_on_day(date(2010, 1, 20), 1)

        EXPECTED = {
            date(2009, 12, 28): 3,
            date(2010, 1, 4):   1,
            date(2010, 1, 11):  0,
            date(2010, 1, 18):  1,
            date(2010, 1, 25):  0
        }
        self.assertEqual(EXPECTED, Entry.cache.weekly_counts(created=rrule(WEEKLY, dtstart=start, until=end)))
        self.assertEqual(EXPECTED, Entry.cache.weekly_counts(created=rrule(WEEKLY, dtstart=start, until=end)))

    def test_monthly_counters(self):
        start = date(2010, 1, 1)
        end = date(2010, 3, 1)
        self.create_entry_on_day(date(2010, 1, 1), 3)
        self.create_entry_on_day(date(2010, 1, 4), 1)
        self.create_entry_on_day(date(2010, 3, 15), 9)

        EXPECTED = {
            date(2010, 1, 1): 4,
            date(2010, 2, 1): 0,
            date(2010, 3, 1): 9,
        }
        self.assertEqual(EXPECTED, Entry.cache.monthly_counts(created=rrule(MONTHLY, dtstart=start, until=end)))
        self.assertEqual(EXPECTED, Entry.cache.monthly_counts(created=rrule(MONTHLY, dtstart=start, until=end)))

    def test_yearly_counters(self):
        start = date(2009, 1, 1)
        end = date(2010, 3, 1)
        self.create_entry_on_day(date(2009, 1, 1), 3)
        self.create_entry_on_day(date(2009, 1, 4), 1)
        self.create_entry_on_day(date(2010, 12, 15), 9)

        EXPECTED = {
            date(2009, 1, 1): 4,
            date(2010, 1, 1): 9,
        }
        self.assertEqual(EXPECTED, Entry.cache.yearly_counts(created=rrule(YEARLY, dtstart=start, until=end)))
        self.assertEqual(EXPECTED, Entry.cache.yearly_counts(created=rrule(YEARLY, dtstart=start, until=end)))

    def test_decrement_counter(self):
        self.assertEqual(1, Entry.cache.count())
        self.assertEqual(1, Entry.cache.daily_count(created=self.entry.created))

        Entry.cache.add(blog=self.blog, title='Blah blah')
        self.assertEqual(2, Entry.cache.count())
        self.assertEqual(2, Entry.cache.daily_count(created=self.entry.created))

        Entry.cache.delete(blog=self.blog, title='Blah blah')
        self.assertEqual(1, Entry.cache.count())
        self.assertEqual(1, Entry.cache.daily_count(created=self.entry.created))

        # Now test with soft deletes
        self.assertEqual(0, Tag.cache.count(entry=self.entry, color=1))
        tag = Tag.cache.add(entry=self.entry, name='buritos', color=1)
        self.assertEqual(1, Tag.cache.count(entry=self.entry, color=1))
        self.assertEqual(1, Tag.cache.yearly_count(created=tag.created))
        Tag.cache.delete(entry=self.entry, name='buritos', color=1)
        self.assertEqual(0, Tag.cache.count(entry=self.entry, color=1))
        self.assertTrue(Tag.cache.mc.delete('Tag:v1:_counter=total:color=1:entry=1'))
        self.assertTrue(Tag.cache.mc.delete('Tag:v1:_counter=yearly.%d' % tag.created.year))
        self.assertEqual(0, Tag.cache.count())
        self.assertEqual(0, Tag.cache.count(entry=self.entry, color=1))
        self.assertEqual(0, Tag.cache.yearly_count(created=tag.created))
        

    def create_entry_on_day(self, day, count):
        for i in range(0, count):
            o = self.blog.entries.create(title=('%s.%d' % (day.isoformat(), i)))
            o.created = day
            o.save()
        
