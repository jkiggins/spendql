import os
import tempfile
import re

from . import spendql
from . import tags

__all__ = ['spendql', 'setup', 'tag', 'display', 'transact']

def einput(default=None, editor='vim'):
    ''' like the built-in input(), except that it uses a visual
    text editor for ease of editing. Unline raw_input() it can also
    take a default value. '''

    with tempfile.NamedTemporaryFile(mode='r+') as tmpfile:

        if default:
            tmpfile.write(default)
            tmpfile.flush()

        child_pid = os.fork()
        is_child = child_pid == 0

        if is_child:
            os.execvp(editor, [editor, tmpfile.name])
        else:
            os.waitpid(child_pid, 0)
            tmpfile.seek(0)
            return tmpfile.read().strip()

# Setup Tags
spendql.trigger(tags.tag, re.compile('INSERT\s+INTO\s+TRANSACT'))