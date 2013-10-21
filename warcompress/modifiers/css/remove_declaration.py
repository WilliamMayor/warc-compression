import random
import cssutils


def modify(text, percentage=0.1):
    sheet = cssutils.parseString(text)
    styleRules = filter(
        lambda r: r.type == cssutils.css.CSSRule.STYLE_RULE,
        sheet.cssRules
    )
    total = 0
    for rule in styleRules:
        total += len(rule.style.keys())
    to_remove = random.sample(xrange(0, total), int(total * percentage))
    i = total - 1
    for rule in styleRules[::-1]:
        for key in list(rule.style.keys())[::-1]:
            if i in to_remove:
                rule.style.removeProperty(key)
            i -= 1
    return sheet.cssText
