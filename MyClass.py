from pylint.lint import Run
from pprint import pprint

results = Run(['C:/Users/offic/PycharmProjects/testing_n_poc/new/t2.py'], do_exit=False)
# `exit` is deprecated, use `do_exit` instead
pprint(results.linter.stats['global_note'])