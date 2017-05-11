import re

from commands import spendql
from commands import router
import spend_input

if __name__ == '__main__':
    command_pattern = re.compile('^([a-z\.]+)', re.I | re.M)
    parameters = re.compile("\s([a-z0-9_]+)\s*\=\s*(\'[^']+\'|[\S]+)", re.I | re.M)
    multiline = re.compile('(\:)')

    cmd_line = spend_input.CommandInput('spend# ', multiline, {'command': command_pattern, 'parameters': parameters})

    loop = True

    while loop:
        cmd_parts = cmd_line.input()
        cmd = cmd_parts['command'][0][0]
        if (cmd_parts is None) or (cmd == 'exit'):
            loop = False

        else:
            if(router.hasCommand(cmd)):
                router.executeCommand(cmd, dict(cmd_parts['parameters']))
            else:
                print("No such module {0}".format(cmd))


    spendql.closeAllConns()
