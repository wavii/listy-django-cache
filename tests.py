#This file mainly exists to allow python setup.py test to work.

import os, sys
os.environ['DJANGO_SETTINGS_MODULE'] = 'test_django_app.settings'
test_dir = os.path.dirname(__file__)
sys.path.insert(0, test_dir)

from django.test.utils import get_runner
from django.conf import settings

def run():
    TestRunner = get_runner(settings)
    failures = TestRunner(verbosity=1, interactive=True).run_tests([])
    sys.exit(failures)
