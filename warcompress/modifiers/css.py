import random
import cssutils


class CSS:

    def all_selectors(self, sheet):
        selectors = []
        sr = sheet.cssRules.rulesOfType(cssutils.css.CSSRule.STYLE_RULE)
        for r in sr:
            sl = r.selectorList
            selectors.append(sl.selectorText)
        return selectors

    def all_declarations(self, sheet):
        declarations = []
        sr = sheet.cssRules.rulesOfType(cssutils.css.CSSRule.STYLE_RULE)
        for r in sr:
            declarations += r.style.children()
        return declarations

    def rule_sizes(self, sheet):
        sizes = []
        sr = sheet.cssRules.rulesOfType(cssutils.css.CSSRule.STYLE_RULE)
        for r in sr:
            sizes.append(len(list(r.style.children())))
        return sizes


class CssDelete:

    def __init__(self, to_remove=1, repetitions=20):
        self.to_remove = int(to_remove)
        self.repetitions = int(repetitions)
        self.name = ('CssDelete '
                     '%d declaration(s) '
                     '%d time(s)') % (self.to_remove, self.repetitions)

    def modify(self, text):
        sheet = cssutils.parseString(text)
        yield sheet.cssText
        for _ in xrange(0, self.repetitions):
            styleRules = filter(
                lambda r: r.type == cssutils.css.CSSRule.STYLE_RULE,
                sheet.cssRules
            )
            total = 0
            for rule in styleRules:
                total += len(rule.style.keys())
            i = total - 1
            remove_indexes = random.sample(
                xrange(0, total),
                min(total, self.to_remove)
            )
            for rule in styleRules[::-1]:
                for key in list(rule.style.keys())[::-1]:
                    if i in remove_indexes:
                        rule.style.removeProperty(key)
                    i -= 1
            yield sheet.cssText


class CssInsertDeclaration(CSS):

    def __init__(self, declarations=1, repeat=20):
        self.declarations = int(declarations)
        self.repeat = int(repeat)
        self.name = ('CssInsertDeclaration '
                     '%d declarations(s) '
                     '%d time(s)') % (self.declarations, self.repeat)

    def modify(self, text):
        sheet = cssutils.parseString(text)
        yield sheet.cssText
        rules = list(
            sheet.cssRules.rulesOfType(cssutils.css.CSSRule.STYLE_RULE)
        )
        for _ in xrange(0, self.repeat):
            rule = random.choice(rules)
            decls = self.all_declarations(sheet)
            for _ in xrange(0, self.declarations):
                n = random.choice(decls)
                rule.style.setProperty(
                    name=n.name,
                    value=n.value,
                    replace=False
                )
            yield sheet.cssText


class CssInsertRule(CSS):

    def __init__(self, rules=1, repeat=20):
        self.rules = int(rules)
        self.repeat = int(repeat)
        self.name = ('CssInsertRule '
                     '%d rules(s) '
                     '%d time(s)') % (self.rules, self.repeat)

    def modify(self, text):
        sheet = cssutils.parseString(text)
        yield sheet.cssText
        sizes = self.rule_sizes(sheet)
        selectors = self.all_selectors(sheet)
        decls = self.all_declarations(sheet)
        for _ in xrange(0, self.repeat):
            style = cssutils.css.CSSStyleDeclaration()
            for _ in xrange(0, random.choice(sizes)):
                decl = random.choice(decls)
                style.setProperty(
                    name=decl.name,
                    value=decl.value,
                    replace=False
                )
            selector = random.choice(selectors)
            rule = cssutils.css.CSSStyleRule(
                selectorText=selector,
                style=style
            )
            sheet.insertRule(rule)
            yield sheet.cssText
