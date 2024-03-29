#! /usr/bin/env python

"""When test classes or functions (as in `pytest-describe`) are deeply nested, the output of `pytest -v` is bulky, repetitive,
and difficult to read.

This script transforms the output of `pytest` into a more readable format like that produced by `rspec` or `jasmine`:

- Repetition is factored out into a hierarchial list-like presentation.
- Identifiers of test classes and functions are printed with spaces in place of underscores
  (e.g., `the_unicorn_has_exactly_one_horn` prints as "the unicorn has exactly one horn");
- Identifiers beginning with `describe` have the `describe` omitted in printing: This makes `pytest-describe` context
  functions read more smoothly.
- Test results (success, failure, etc.) are also signalled for easier identification by a symbol at the front of
  each test listing, as well as the `pytest` standard code in parentheses at the end.

Anything in the output file that is not a test result line is passed through unchanged.
A test result line is any line that occurs between the '======== ...' boundary markers, and that contains '::'.
The accumulated test results are printed as a hierarchical list after the closing boundary marker.

Usage: pytest -v ... | python scripts/format-pytest.py
"""

import sys
import re


test_regex = re.compile(r"(.+) (\w+)$")


def is_test(string):
    return re.match(test_regex, string)


def test_parts(string):
    return re.match(test_regex, string).group(1, 2)  # name, state


state_glyphs = {"PASSED": "-", "FAILED": "X", "ERROR": "E", "SKIPPED": "o"}


def state_glyph(state):
    return state_glyphs.get(state, "?")


class Describe:
    def __init__(self, name):
        self.name = name
        self.describes = []
        self.tests = []

    def empty(self):
        return len(self.describes) == 0 and len(self.tests) == 0

    def add_describe(self, describe):
        self.describes.append(describe)

    def add_test(self, test):
        self.tests.append(test)

    def add_components(self, components):
        """Add the components specified in the list `component`, starting with this node as the root;
        component[0] is first child"""
        first = components.pop(0)
        if is_test(first):
            self.add_test(Test(*(test_parts(first))))
        else:
            describe = next((d for d in self.describes if d.name == first), None)
            if not describe:
                describe = Describe(first)
                self.add_describe(describe)
            describe.add_components(components)

    def rep(self):
        result = self.name
        for pattern, replace in [
            (r"^describe_", r""),
            (r"__", r"*"),
            (r"_", r" "),
            (r"\*", r"_"),
        ]:
            result = re.sub(pattern, replace, result)
        return result

    def lines(self, indent):
        result = [self.rep()]
        result.extend(
            [("%s%s" % (indent, l)) for d in self.describes for l in d.lines(indent)]
        )
        result.extend(
            [("%s%s" % (indent, l)) for t in self.tests for l in t.lines(indent)]
        )
        return result


class Test:
    def __init__(self, name, state):
        self.name = name
        self.state = state

    def rep(self):
        result = self.name
        for pattern, replace in [(r"__", r"*"), (r"_", r" "), (r"\*", r"_")]:
            result = re.sub(pattern, replace, result)
        return "%s %s (%s)" % (state_glyph(self.state), result, self.state)

    def lines(self, indent):
        return [self.rep()]


if __name__ == "__main__":
    test_summary_boundary = re.compile(r"=+ .* =+")
    in_test_summary = False

    root = Describe("TESTS:")

    for line in sys.stdin.readlines():
        if test_summary_boundary.match(line):
            in_test_summary = not in_test_summary
        if in_test_summary and "::" in line:
            root.add_components(re.split("::", line))
        else:
            sys.stdout.write(line)

    if not root.empty():
        sys.stdout.write("\n".join(root.lines("   ")))
        sys.stdout.write("\n")
