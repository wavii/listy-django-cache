import cPickle as pickle
import unittest
import logging

log = logging.getLogger(__name__)

class OnDemandPickle(object):
    """
    Assumes that the object pickled is a sequence. Returns one object
    at a time as an iterator and deserializes the provided file-like
    object on demand.
    """
    def __init__(self, f, protocol=pickle.HIGHEST_PROTOCOL, pre_serialization=None, post_deserialization=None):
        self.f = f
        self.protocol = protocol
        self.pre_serialization = pre_serialization
        self.post_deserialization = post_deserialization
        OnDemandPickle.last_size = len(self.f.getvalue())
        OnDemandPickle.last_data = self.f.getvalue()
        log.debug('Total pickled size: %d bytes', OnDemandPickle.last_size)

    def load(self):
        while True:
            try:
                o = pickle.load(self.f)
                if self.post_deserialization:
                    o = self.post_deserialization(o)
                yield o
            except EOFError:
                raise StopIteration

    def dump(self, value):
        if getattr(value, '__iter__', False):
            for o in value:
                o = self.pre_serialization(o) if self.pre_serialization else o
                pickle.dump(o, self.f, self.protocol)
        else:
            o = self.pre_serialization(value) if self.pre_serialization else value
            pickle.dump(o, self.f, self.protocol)


class InspectorPickle(object):
    "Only used for inspecting the innards of a memcache entry"
    def __init__(self, f, protocol=pickle.HIGHEST_PROTOCOL):
        self.f = f
        self.protocol = protocol
        InspectorPickle.last_value = self.f.getvalue()

    def load(self):
        while True:
            try:
                yield pickle.load(self.f)
            except EOFError:
                raise StopIteration

    def dump(self, value):
        raise RuntimeError('Not implemented')


class OnDemandPickleTest(unittest.TestCase):
    def test_load(self):
        import cStringIO, types

        f = cStringIO.StringIO()
        pickle.dump(23, f)
        pickle.dump(56, f)

        f = cStringIO.StringIO(f.getvalue())
        p = OnDemandPickle(f)
        self.assertTrue(isinstance(p.load(), types.GeneratorType))
        self.assertEqual([23, 56], list(p.load()))

    def test_dump(self):
        import cStringIO, types

        f = cStringIO.StringIO()
        p = OnDemandPickle(f)
        p.dump([23, 56])

        f = cStringIO.StringIO(f.getvalue())
        p = OnDemandPickle(f)
        self.assertTrue(isinstance(p.load(), types.GeneratorType))
        self.assertEqual([23, 56], list(p.load()))

    def test_dump_non_iterable(self):
        import cStringIO, types

        f = cStringIO.StringIO()
        p = OnDemandPickle(f)
        p.dump(97)

        f = cStringIO.StringIO(f.getvalue())
        p = OnDemandPickle(f)
        self.assertTrue(isinstance(p.load(), types.GeneratorType))
        self.assertEqual([97], list(p.load()))

        

if __name__ == '__main__':
    import sys, binascii, string
    from lang_utils.hex import dump_hex
    count = len(sys.argv)
    if count > 1:
        if count > 2 and sys.argv[1] == 'inspect':
            key = sys.argv[2]
            address = sys.argv[3] if count == 4 else '127.0.0.1:11211'
            import memcache
            mc = memcache.Client([address],
                                 pickleProtocol=pickle.HIGHEST_PROTOCOL,
                                 pickler=OnDemandPickle,
                                 unpickler=InspectorPickle)
            o = mc.get(key)
            if o is not None:
                print dump_hex(InspectorPickle.last_value)
            else:
                print "Key doesn't exist: %r" % key
            quit()
        print 'Usage: inspect <key> [<memcache address>]'
        quit()
            

    unittest.main()
