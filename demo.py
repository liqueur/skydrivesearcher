#coding:utf-8

import re

def replace(matched):
    return '\{escape}'.format(escape=matched.group('escape'))

rs = re.sub(r'(?P<escape>[-+!\\():^\]\[{}~*?])', replace, '[jojo]')
print rs
