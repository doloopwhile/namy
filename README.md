vrename
=======

Rename files by editor

Example
-------

There are files named by a rule

    $ ls
    foo1 foo2 foo3

collect and edit filename (by Vi editor)

    $ EDITOR=vi vrename start foo*

edit filename (by your favirite editor)

    ohLkYU foo1
    MzUkcU foo2
    Glw4Gu foo3

editted

    ohLkYU bar1
    MzUkcU bar2
    Glw4Gu bar3

run `vrename move`

    vrename move

rename done

    $ ls
    bar1 bar2 bar3
