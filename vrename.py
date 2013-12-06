#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
import shutil
import string
import math
import random
from os.path import (
    abspath,
    join,
)

from argparse import ArgumentParser

from collections import namedtuple
from subprocess import (
    check_call,
    CalledProcessError,
)
import shlex
from collections import OrderedDict
import traceback
from contextlib import contextmanager

EXIT_SUCCESS = 0
EXIT_FAIL = 1
EXIT_UNEXPECTED_FAIL = 2

MIN_TAG_LENGTH = 6
TAG_CHARSET = list(
    string.ascii_lowercase
    + string.ascii_uppercase
    + string.digits
)


DirStats = namedtuple('DirStats', 'dir_path old_path new_path')


def get_dir_stats(name):
    dir_path = join('/tmp/vrename/', name)
    return DirStats(
        dir_path=abspath(dir_path),
        old_path=abspath(join(dir_path, 'old.txt')),
        new_path=abspath(join(dir_path, 'new.txt')),
    )


def which_editor():
    editor = os.getenv('EDITOR') or os.getenv('VISUAL')
    if not editor:
        return 'vi'
    return editor


def open_editor(path):
    check_call(shlex.split(which_editor()) + [path], shell=False)


def open_editor_and_exit_if_fail(path):
    try:
        open_editor(path)
    except CalledProcessError as e:
        print(
            'editor command {} exited with code {}'.format(
                e.cmd, e.returncode),
            file=sys.stderr
        )
        sys.exit(EXIT_FAIL)


def random_tagging(items, min_tag_length, charset):
    items = list(items)
    tag_length = int(math.ceil(math.log(len(items), len(charset)))) + 1
    if tag_length < min_tag_length:
        tag_length = min_tag_length

    def iter_random_tags():
        tags = set()
        while 1:
            t = ''.join(random.choice(charset) for _ in range(tag_length))
            if t not in tags:
                tags.add(t)
                yield t

    return zip(iter_random_tags(), items)


def parse_old_new_file(dirstats):
    def _parse_old_new_file(path):
        with open(path, encoding=sys.getfilesystemencoding()) as fp:
            for line in fp:
                tag, path = line.strip().split(None, 1)
                yield (tag, path)

    return (
        list(_parse_old_new_file(dirstats.old_path)),
        list(_parse_old_new_file(dirstats.new_path)),
    )


class DuplicateTagError(KeyError):
    def __init__(self, tag, left_values):
        super().__init__(tag, left_values)
        self.tag = tag
        self.left_values = left_values


class NoLeftValueError(KeyError):
    def __init__(self, tag, right_value):
        super().__init__(tag, right_value)
        self.tag = tag
        self.right_value = right_value


class MultipleRightValueError(KeyError):
    def __init__(self, tag, left_value, right_values):
        super().__init__(tag, left_value, right_values)
        self.tag = tag
        self.left_value = left_value
        self.right_values = right_values


class NoRightValueError(KeyError):
    def __init__(self, tag, left_value):
        super().__init__(tag, left_value)
        self.tag = tag
        self.left_value = left_value


def _confront_two_tables(left_table, right_table):
    d = OrderedDict()
    for tag, left_value in left_table:
        if tag in d:
            raise DuplicateTagError(tag, [d[tag][0], left_value])
        d[tag] = (left_value, [])

    for tag, right_value in right_table:
        try:
            d[tag][1].append(right_value)
        except KeyError:
            raise NoLeftValueError(tag, right_value)

    return d.items()


def confront_two_tables(left_table, right_table):
    """
    >>> confront_two_tables(
    ... [("tag1", "L1"), ("tag2", "L2"), ("tag3", "L3")],
    ... [("tag1", "R1"), ("tag1", "R1-B"), ("tag3", "R3")],
    ... )
    [('L1', ['R1', 'R1-B']), ('L2', []), ('L3', ['R3'])]

    >>> confront_two_tables(
    ...     [("tag1", "L1"), ("tag1", "L1-B")],
    ...     []
    ... )
    Traceback (most recent call last):
    vrename.DuplicateTagError: ('tag1', ['L1', 'L1-B'])

    >>> confront_two_tables(
    ...     [("tag1", "v"), ("tag2", "v")],
    ...     [('tag3', 'missing-left')],
    ... )
    Traceback (most recent call last):
    vrename.NoLeftValueError: ('tag3', 'missing-left')
    """
    items = []
    for tag, (left, rights) in _confront_two_tables(left_table, right_table):
        items.append((left, rights))
    return items


def pairwise_two_tables(left_table, right_table, allow_no_right=True):
    """
    >>> pairwise_two_tables(
    ...     [("tag1", "L1"), ("tag2", "L2"), ("tag3", "L3")],
    ...     [("tag1", "R1"), ("tag3", "R3"), ("tag2", "R2")],
    ... )
    [('L1', 'R1'), ('L2', 'R2'), ('L3', 'R3')]

    >>> pairwise_two_tables(
    ...     [("tag1", "L1"), ("tag2", "L2")],
    ...     [("tag1", "R1"), ("tag3", "R3"), ("tag2", "R2")],
    ... )
    Traceback (most recent call last):
    vrename.NoLeftValueError: ('tag3', 'R3')

    >>> pairwise_two_tables(
    ...     [("tag1", "L1"), ("tag2", "L2"), ("tag3", "L3")],
    ...     [("tag1", "R1"), ("tag3", "R3")],
    ...     False,
    ... )
    Traceback (most recent call last):
    vrename.NoRightValueError: ('tag2', 'L2')

    >>> pairwise_two_tables(
    ...     [("tag1", "L1"), ("tag2", "L2"), ("tag3", "L3")],
    ...     [("tag1", "R1"), ("tag3", "R3")],
    ... )
    [('L1', 'R1'), ('L2', None), ('L3', 'R3')]

    >>> pairwise_two_tables(
    ...     [("tag1", "L1"), ("tag1", "L1-B")],
    ...     []
    ... )
    Traceback (most recent call last):
    vrename.DuplicateTagError: ('tag1', ['L1', 'L1-B'])

    >>> pairwise_two_tables(
    ...     [("tag1", "L1"), ("tag2", "L2"), ("tag3", "L3")],
    ...     [("tag1", "R1"), ("tag3", "R3"), ("tag2", "R2"), ("tag1", "R1-B")],
    ... )
    Traceback (most recent call last):
    vrename.MultipleRightValueError: ('tag1', 'L1', ['R1', 'R1-B'])
    """
    pairs = []
    for tag, (left, rights) in _confront_two_tables(left_table, right_table):
        if len(rights) > 1:
            raise MultipleRightValueError(tag, left, rights)
        if not rights:
            if allow_no_right:
                pairs.append((left, None))
            else:
                raise NoRightValueError(tag, left)
        else:
            pairs.append((left, rights[0]))
    return pairs


@contextmanager
def capture_os_error():
    # for backward compatibility, catch EnvironmentError
    # in Python 3.3 >=, EnvironmentError and IOError are merged into OSError
    # and are alias of OSError
    try:
        yield
    except EnvironmentError as err:
        print_error_and_exit(str(err))


def print_error_and_exit(message):
    print(message, file=sys.stderr)
    sys.exit(EXIT_FAIL)

#==============================================================================
# main functions
#==============================================================================


def main_start(args):
    d = get_dir_stats(args.name)

    with capture_os_error():
        check_call(['rm', '-rf', d.dir_path])
        os.makedirs(d.dir_path)

        with open(d.old_path, 'w', encoding=sys.getfilesystemencoding()) as fp:
            tag_paths = random_tagging(args.files, MIN_TAG_LENGTH, TAG_CHARSET)
            for tag, path in tag_paths:
                print(tag, path, file=fp)

        shutil.copyfile(d.old_path, d.new_path)

    if args.edit:
        open_editor_and_exit_if_fail(d.new_path)


def main_edit(args):
    d = get_dir_stats(args.name)
    open_editor_and_exit_if_fail(d.new_path)


def main_move(args):
    d = get_dir_stats(args.name)
    with capture_os_error():
        old_path_tags, new_path_tags = parse_old_new_file(d)

    def process(old_path, new_path):
        if new_path is None:
            os.remove(old_path)
        else:
            os.renames(old_path, new_path)

    def message(old_path, new_path):
        if new_path is None:
            return 'Delete {}'.format(old_path)
        else:
            return 'Renamed {} to {}'.format(old_path, new_path)

    def format_paths_message(text_path, tag, file_paths):
        msg = "\nIn {}".format(text_path)
        if not file_paths:
            msg += "\n  (Not Found)"
        else:
            for p in file_paths:
                msg += "\n  {} {}".format(tag, p)
        return msg

    try:
        old_new_pairs = pairwise_two_tables(old_path_tags, new_path_tags)
    except DuplicateTagError as err:
        msg = 'Multiple source paths with same tag found'
        msg += format_paths_message(d.old_path, err.tag, err.left_values)
        print_error_and_exit(msg)
    except NoLeftValueError as err:
        msg = 'No corresponding source path found'
        msg += format_paths_message(d.old_path, err.tag, [])
        msg += format_paths_message(d.new_path, err.tag, [err.right_value])
        print_error_and_exit(msg)
    except MultipleRightValueError as err:
        msg = 'Multiple corresponding destination paths found'
        msg += format_paths_message(d.old_path, err.tag, [err.left_value])
        msg += format_paths_message(d.new_path, err.tag, err.right_values)
        print_error_and_exit(msg)

    for old_path, new_path in old_new_pairs:
        with capture_os_error():
            print(message(old_path, new_path), file=sys.stdout)
            sys.stdout.flush()
            if args.dry_run:
                continue
            process(old_path, new_path)


def main_copy(args):
    d = get_dir_stats(args.name)
    with capture_os_error():
        old_path_tags, new_path_tags = parse_old_new_file(d)

    def process(old_path, new_path):
        shutil.copyfile(old_path, new_path)

    def message(old_path, new_path):
        return 'Copied {} to {}'.format(old_path, new_path)

    def format_paths_message(text_path, tag, file_paths):
        msg = "\nIn {}".format(text_path)
        if not file_paths:
            msg += "\n  (Not Found)"
        else:
            for p in file_paths:
                msg += "\n  {} {}".format(tag, p)
        return msg

    try:
        old_new_pairs = confront_two_tables(old_path_tags, new_path_tags)
    except DuplicateTagError as err:
        msg = 'Multiple source paths with same tag found'
        msg += format_paths_message(d.old_path, err.tag, err.left_values)
        print_error_and_exit(msg)
    except NoLeftValueError as err:
        msg = 'No corresponding source path found'
        msg += format_paths_message(d.old_path, err.tag, [])
        msg += format_paths_message(d.new_path, err.tag, [err.right_value])
        print_error_and_exit(msg)

    for old_path, new_paths in old_new_pairs:
        for new_path in new_paths:
            with capture_os_error():
                print(message(old_path, new_path), file=sys.stdout)
                sys.stdout.flush()
                if args.dry_run:
                    continue
                process(old_path, new_path)


def main():
    try:
        parser = ArgumentParser()

        subparsers = parser.add_subparsers(title='subcommands')
        start_parser = subparsers.add_parser('start')
        start_parser.add_argument('--name', default='default')
        start_parser.add_argument('--no-edit', action='store_false',
                                  dest='edit')
        start_parser.add_argument('files', nargs='+')
        start_parser.set_defaults(func=main_start, edit=True)

        edit_parser = subparsers.add_parser('edit')
        edit_parser.add_argument('--name', default='default')
        edit_parser.set_defaults(func=main_edit)

        move_parser = subparsers.add_parser('move')
        move_parser.add_argument('--name', default='default')
        move_parser.add_argument('-n', '--dry-run', action='store_true',
                                 dest='dry_run')
        move_parser.set_defaults(func=main_move)

        move_parser = subparsers.add_parser('copy')
        move_parser.add_argument('--name', default='default')
        move_parser.add_argument('-n', '--dry-run', action='store_true',
                                 dest='dry_run')
        move_parser.set_defaults(func=main_copy)

        args = parser.parse_args()
        args.func(args)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(EXIT_UNEXPECTED_FAIL)


if __name__ == '__main__':
    main()
