from django.db import models
from listy import CachingManager

class Blog(models.Model):
    name = models.CharField(max_length=32)
    description = models.TextField()

class Entry(models.Model):
    blog = models.ForeignKey(Blog, related_name='entries')
    title = models.CharField(max_length=32, unique=True)
    created = models.DateTimeField(auto_now_add=True)

    objects = CachingManager([('pk',), ('title',)], timestamp_field='created')

    def __unicode__(self):
        return '%d' % self.id

class Tag(models.Model):
    entry = models.ForeignKey(Entry, related_name='tags')
    name = models.CharField(max_length=32)
    color = models.IntegerField(choices=list(enumerate(['red', 'green', 'blue', 'yellow'])))
    enabled = models.BooleanField(default=True)
    deleted = models.BooleanField(default=False)
    deleted_timestamp = models.DateTimeField(null=True)
    created = models.DateTimeField(auto_now_add=True)

    objects = CachingManager([('entry', 'color'), ('name', 'color'), ('pk',)],
                             soft_delete_field='deleted',
                             deleted_timestamp_field='deleted_timestamp',
                             enabled_field='enabled',
                             timestamp_field='created',
                             )

    def __unicode__(self):
        return '%r, %r, %r, %r, %r, %r' % (self.entry, self.name, self.color, self.enabled, self.deleted, self.deleted_timestamp)


