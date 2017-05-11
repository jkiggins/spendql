import re

class CommandInput:

    pattern = None
    validator = None
    prefix = ''
    multiline = None

    # prefix: the prefix for each input line
    # multiline: a regex to determine whether the command contains multiline input or not, the pattern should only match and unfinished multiline input block
    # patterns: a dictionary containing patters to partition the command, a similar dicationary will be returned containing tuples representing the various match groups
    def __init__(self, prefix, multiline, patterns):
        self.patterns = patterns
        self.prefix = prefix
        self.multiline = multiline

    def partOut(self, cmd):
        parts = {}
        match_len = 0

        for key in self.patterns:
            parts[key] = []

            for m in self.patterns[key].finditer(cmd):
                parts[key].append(m.groups())

        return parts


    def cmdError(self, cmd):
        print('error in input, must be of the form:\ncommand param0=value0 param1 = value1 ...')


    def input(self):

        print(self.prefix, end='')

        loop = True
        cmd = ''

        while loop:
            cmd += input()

            loop = False
            ml = self.multiline.findall(cmd)
            if ml is not None:
                if (len(ml) & 1) == 1:
                    loop = True

        cmd = self.multiline.sub('', cmd)

        parts = self.partOut(cmd)

        return parts


def test():
    """This method tests the CommandInput class"""
    command_pattern = re.compile('^([a-z]+(?=\s{0,1}))', re.I | re.M)
    parameters = re.compile('\s([a-z0-9_]+)\=([a-z0-9_\n]+)', re.I | re.M)
    multiline = re.compile('(\:)')

    cmd_line = CommandInput('spend# ', multiline, {'command': command_pattern, 'parameters': parameters})

    # parts = cmd_line.partOut('print msg=hello_world25')
    # print(parts)

    print("my input\n========\n{0}".format(cmd_line.input()))


if __name__ == '__main__':
    test()
	