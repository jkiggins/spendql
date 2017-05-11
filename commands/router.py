import re
import inspect


commands = []

# global variables to support calling stack
index = 0
calls = []
params = {}


def route(rx, call):
    if(callable(call)):
        rx_compiled = re.compile(rx)
        commands.append({'rx': rx_compiled, 'call': call})
    else:
        raise TypeError('argument passed to parameter "call" with type: ' + type(call) + ' is not callable')


def callNext():
    global index
    index += 1

    if index < len(calls):
        args = inspect.getargspec(calls[0]).args
        call_params = {}

        call_params = {k: params[k] for k in params.keys() & args}
        
        calls[0](**call_params)


def executeCallableList(arg_calls, arg_params):
    if len(calls) > 0:
        # Reset globals for command stack
        global index 
        global calls
        global params

        index = -1
        calls = arg_calls
        params = arg_params

        params['next'] = callNext  # set the next parameter
        
        # start the sequence
        callNext()


def getMatchesList(test, dlist):
    matches = []
    for item in dlist:
        if item['rx'].fullmatch(test) is not None:
            matches.append(item['call'])
    
    return matches     


def send(name, *args):
    params = {}

    if len(args) >= 1:
        params = args[0]

    matches = getMatchesList(name, commands)
    executeCallableList(matches, params)