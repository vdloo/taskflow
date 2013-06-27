import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

if not os.path.isfile(os.path.join(os.getcwd(), "taskflow", '__init__.py')):
    sys.path.insert(0, os.path.join(os.path.abspath(os.getcwd()), os.pardir))
else:
    sys.path.insert(0, os.path.abspath(os.getcwd()))

from taskflow import decorators

from taskflow.patterns import linear_flow as lf


def undo_call(context, result, cause):
    print("Calling %s and apologizing." % result)


@decorators.task(revert_with=undo_call)
def call_jim(context):
    print("Calling jim.")
    print("Context = %s" % (context))
    return context['jim_number']


@decorators.task(revert_with=undo_call)
def call_joe(context):
    print("Calling joe.")
    print("Context = %s" % (context))
    return context['joe_number']


def call_suzzie(context):
    raise IOError("Suzzie not home right now.")


flow = lf.Flow("call-them")
flow.add(call_jim)
flow.add(call_joe)
flow.add(call_suzzie)

context = {
    "joe_number": 444,
    "jim_number": 555,
}

try:
    flow.run(context)
except Exception as e:
    print("Flow failed: %s" % (e))
