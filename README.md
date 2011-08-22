Introduction
============

Listy is a deterministic caching mechanism for django projects. It
will keep the cache in-sync with the database by updating during
changes instead of relying on timeouts. As implied by the name, Listy
supports looking up lists of objects.

Features
========

* Very easy to use
* Deterministic
* Fast access to lists of things
* Packs data into compact form (most small records go from about 1k down to 10s of bytes)
* On-demand deserialization
* Use of memcache's prepend command for fast adds
* Keeps track of total, yearly, monthly, weekly, and daily counts for each cached list
* Optional per-request object registry
* Optional support for soft deletes

Usage
=====

Using Listy is as simple as replacing the default model manager with a
CachingManager and providing it with the list of keys that you will
want to query with.
    
In this contrived example, I can look up based on `pk`, just the
`follower`, or both the `follower` and `followee`:

    import listy
    
    class Follow(models.Model):
        # Configure this cache to support lookup by 'pk' or 'follower'
        objects = listy.CachingManager([('pk',), ('follower',), ('follower', 'followee')])
        follower = models.ForeignKey('User', related_name='follows')
        followee = models.ForeignKey('User', related_name='followers')
    
    # Follow someone
    Follow.cache.add(follower=me, followee=you)
    
    # Unfollow someone
    Follow.cache.delete(follower=me, followee=you)
    
    # Get the users I follow
    Follow.cache.get(follower=me)

    # Get whether I follow you
    Follow.cache.get(follower=me, follower=you)

    # Get the counts for the users I follow
    from datetime import date, timedelta
    from dateutil.rrule import rrule, DAILY, WEEKLY, MONTHLY, YEARLY
    start, end = date.today() - timedelta(days=100), date.today()
    Follow.cache.daily_counts(created=rrule(DAILY, dtstart=start, until=end))

You can see a complete set of examples in `test_django_app/caching/tests.py`.

Arguments to CachingManager:

* `caches` -- a list of tuples describing the fields that should be keys into the cache
* `soft_delete_field` -- the name of the delete field that can be used to delete objects without actually removing them from the database if this feature is supported by the model (default None)
* `deleted_timestamp_field` -- the name of the field which should be set to a datetime when deleting an object (default None)
* `enabled_field` -- the name of the field which defines whether an object is enabled or not, this is treated like a delete that cannot be undone under normal circumstances (default None)
* `timestamp_field` -- the name of the field that hold the timestamp to be used for the counters (default None)
* `disable_cache` -- turn off caching, can be used for debugging (default False)
* `address` -- a function that returns the address of the memcache (default 127.0.0.1:11211)
* `filter_out_soft_deletes` -- treat soft deletes as true deletes, filter them out when returning lists (default True)

The list of tuples defined by the caches argument is the heart of this
caching mechanism. Through it we define what lists of objects we want
cached and updated, and how we will access those lists.

Motivation and Assumptions
==========================

We want to be able to get lists of thousands of items in the database
without putting too much load on the database server.

Our assumptions going into this project:

* We want to get lists of things from the database
* The data will be read often and changed rarely
* When there is a change it needs to be immediately visible on any Django process on any host
* It's easier to provide a new (and constrained) interface than to attempt to perfectly replicate Django's filter/update interface
* Code using the cached models will only ever go through the cache interface for updates
* Correctness is more important than performance (knowing full well that it isn't going to be perfect)
* The model has a normal primary key called 'id'
* Loose reverse chronological order of the lists is good enough (strict ordering isn't required)
* The keys we use to look stuff up with aren't particularly large
* Memory for memcache is more constrained than cpu
* Any time we can make a tradeoff where we move work or storage from a centralized host (like the database or cache hosts) to somewhere else (like the webservers) we will take that opportunity even if it increases the number of hosts

Details
=======

This is a cache for models where the main piece of information you are
getting out of it is whether something is set or not. It will
absolutely *not* be useful for models that have state that changes
often.

During an update we try to prepend the new value to the list (assuming
reverse order is what we want) and if it fails because the item isn't
there we go and get everything from the database and update the cache.

We maintain multiple caches and update each one when something is
added or deleted. Since adding is the common case and deleting is rare
we optimize for adding rather than deleting. If we ever have a use
case where lots of stuff is added and deleted it would make sense to
support deletion lists in the cache.

During a get we return a generator object that deserializes the
objects as needed. This allows us to maintain large lists in the cache
without the overhead of deserializing every single object. This is
supported via my interesting OnDemandPickle class that takes advantage
of file-like objects and EOFs in the strings returned by memcache. It
works perfectly with memcache's append and prepend commands.

Note that we can get into a state where we have multiple copies of an
object with different states. If this happens the earlier object in
the generator is the most recent and should generally be considered
the correct one.

Note that objects can't be raw integers or longs since those aren't
actually pickled in our memcache client. But that won't be a problem
if you are just caching objects from Django.

Note that while we don't load everything returned by memcache unless
it is being used we do get everything from memcache in a list so this
could be slow if the lists get to large. We should eventually
implement an on-demand getter for memcache.

Dependencies
============

* memcache
* dateutil
* Django 1.2 (in the past it has worked on 1.1, but it's untested on 1.3)

