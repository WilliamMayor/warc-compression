import random
import cssutils


def modify(text, percentage=0.1):
    sheet = cssutils.parseString(text)
    total = len(sheet.cssRules)
    to_remove = random.sample(xrange(0, total), int(total * percentage))
    for i in sorted(to_remove, reverse=True):
        sheet.deleteRule(i)
    return sheet.cssText
